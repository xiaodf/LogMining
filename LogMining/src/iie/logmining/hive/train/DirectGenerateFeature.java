package iie.logmining.hive.train;

import iie.udps.common.hcatalog.SerHCatInputFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.hive.HiveContext;

import scala.Serializable;
import scala.Tuple2;

/*
 * 利用spark读取AddIDS.java生成的表log3中的数据进行聚类加标签后存回新表log4中
 * 
 * spark-submit --class iie.logmining.data.train.DirectGenerateFeature --master yarn-client 
 * --jars /home/xdf/udps-sdk-0.0.1.jar,/home/xdf/jars/lucene-analyzers-common-4.10.2.jar,/home/xdf/spark-mllib_2.10-1.3.0.jar
 *   /home/xdf/test2.jar -c /user/xdf/stdin.xml
 */
public class DirectGenerateFeature implements Serializable {
	private static final long serialVersionUID = 1L;
	public static boolean[] flag;
	public static List<String> label_set_list = new LinkedList<String>();
	public static HashMap<String, String> docid_vector_map = new HashMap<String, String>();
	public static String similar_lables = "";
	public static String feature = "";

	@SuppressWarnings({ "rawtypes"})
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}
		String stdinXml = args[1];
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> parameters = operXML.parseStdinXml(stdinXml);// 参数列表

		String dbName = parameters.get(0).get("dbName").toString(); // 数据库
		// String addIDOutTable = parameters.get(0).get("addIDOutTable")
		// .toString(); // 输入表
		// String featureTable =
		// parameters.get(0).get("featureTable").toString(); // 输出表
		String addIDOutTable = "logs3"; // 输入表
		String featureTable = "logs4"; // 输出表

		final double similarity = Double.parseDouble(parameters.get(0)
				.get("similarity").toString());// 相似系数

		SparkConf sparkConf = new SparkConf()
				.setAppName("DirectGenerateFeature");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		// 读取hive表数据
		Configuration inputConf = new Configuration();
		SerHCatInputFormat.setInput(inputConf, dbName, addIDOutTable);
		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(inputConf, SerHCatInputFormat.class,
						WritableComparable.class, SerializableWritable.class);
		JavaRDD<String> lines = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, String>() {
					private static final long serialVersionUID = -2362812254158054659L;

					public String call(
							Tuple2<WritableComparable, SerializableWritable> v)
							throws Exception {
						HCatRecord record = (HCatRecord) v._2.value();
						String message = record.get(0).toString() + ": "
								+ record.get(1).toString();
						return message;
					}
				}).cache();

		// 获取数据记录的邻接矩阵
		int rowCount = (int) lines.repartition(1).count();
		String[][] matrix = getAdjacentMatrix(lines, similarity, rowCount);

		// 获得临时类别数据每行数据（id，message）
		List<String> rowVector = lines.repartition(1).collect();
		String[] rowIds = new String[rowCount];
		for (int i = 0; i < rowVector.size(); i++) {
			String[] temp = rowVector.get(i).split(": ");
			rowIds[i] = i + "";// 行号数组，深度遍历时用
			docid_vector_map.put(temp[0], temp[1]);
		}

		// 图的深度遍历操作(递归)，聚类
		DFSTraverse(rowCount, rowIds, matrix);

		// 得到已有特征标签库标签总数，用于参考初始化新特征类别定义
		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(
				jsc.sc());
		int featureID = (int) sqlContext.sql(
				"select * from " + dbName + "." + featureTable).count();

		// 将处理后的数据存到输出表中
		storeToTable(dbName, featureTable, featureID);
		
		//删除输入表
		sqlContext.sql("drop table " + dbName + "." + addIDOutTable);
		jsc.stop();
		System.exit(0);
	}

	/**
	 * 获得临时类别数据的邻接矩阵
	 * 
	 * @param lines
	 * @param similarity
	 * @param rowCount
	 * @return
	 */
	public static String[][] getAdjacentMatrix(JavaRDD<String> lines,
			final double similarity, int rowCount) {
		// 生成字符串向量的笛卡尔积
		JavaRDD<String> vector = lines;
		JavaPairRDD<String, String> vector2 = lines.cartesian(vector); // 笛卡尔积

		// 计算两个向量是否相似
		JavaRDD<String> lcsArr = vector2.map(
				new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> arg0)
							throws Exception {
						int flag;
						// 获取两个向量生成数组
						String[] list1 = arg0._1.split(": ");
						String[] list2 = arg0._2.split(": ");
						// 获取行号
						String rowId1 = list1[0];
						String rowId2 = list2[0];
						// 获取内容
						String[] message1 = list1[1].split(" ");
						String[] message2 = list2[1].split(" ");
						// 计算最长公共子序列长度
						int[][] c = new int[message1.length][message2.length];// c[][]存储x、yLCS长度
						for (int p = 1; p < message1.length; p++) {
							for (int q = 1; q < message2.length; q++) {
								if (message1[p].equals(message2[q])) {
									c[p][q] = c[p - 1][q - 1] + 1;
								} else if (c[p - 1][q] >= c[p][q - 1]) {
									c[p][q] = c[p - 1][q];
								} else {
									c[p][q] = c[p][q - 1];
								}
							}
						}
						int lcsLength = c[message1.length - 1][message2.length - 1];
						double rowStr_lcs = (double) lcsLength
								/ message1.length;
						double colStr_lcs = (double) lcsLength
								/ message2.length;
						if (rowStr_lcs >= similarity
								&& colStr_lcs >= similarity) {
							flag = 1;
						} else {
							flag = 0;
						}
						return rowId1 + ";" + rowId2 + ";" + flag;
					}
				}).cache();

		// 获得节点邻接矩阵 .repartition(1)
		List<String> vertexList = lcsArr.collect();
		String[][] matrix = new String[rowCount][rowCount];
		for (int i = 0; i < vertexList.size(); i++) {
			String[] temp = vertexList.get(i).split(";");
			matrix[Integer.parseInt(temp[0])][Integer.parseInt(temp[1])] = temp[2];// 邻接矩阵
		}
		return matrix;
	}

	/**
	 * 将处理后的数据存到输出表中
	 * 
	 * @param dbName
	 * @param featureTable
	 * @throws HCatException
	 */
	public static void storeToTable(String dbName, String featureTable,
			int featureID) throws HCatException {
		// 生成数据（类别，特征，IDS）存到hive表中
		WriteEntity.Builder builder = new WriteEntity.Builder();
		WriteEntity entity = builder.withDatabase(dbName)
				.withTable(featureTable).build();
		Map<String, String> config = new HashMap<String, String>();
		HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
		WriterContext context = writer.prepareWrite();
		HCatWriter splitWriter = DataTransferFactory.getHCatWriter(context);
		List<HCatRecord> records = new ArrayList<HCatRecord>();

		for (int i = 0; i < label_set_list.size(); i++) {
			// 先得到每个类别的包含的行的序号
			String[] docIdArr = label_set_list.get(i).split(" ");
			List<String> list = new ArrayList<String>();
			feature = "P," + docid_vector_map.get(docIdArr[0]);// 初始化
			String docId = docIdArr[0] + " ";
			list.add(docIdArr[0]);
			for (int j = 1; j < docIdArr.length; j++) {
				docId += docIdArr[j] + " ";
				list.add(docIdArr[j]);
				String[] fArr = feature.split(" ");
				String tmp = "P," + docid_vector_map.get(docIdArr[j]);
				String[] coArr = tmp.split(" ");
				LCSAlgorithm(fArr, coArr);
			}
			String LabelName = "L_" + (i + featureID);
			if (feature.length() > 2) {
				// 去掉"P,"
				feature = feature.substring(2);
			}
			List<Object> temp = new ArrayList<Object>(3);
			temp.add(LabelName);
			temp.add(feature);
			temp.add(docId);
			records.add(new DefaultHCatRecord(temp));
		}
		splitWriter.write(records.iterator());
		writer.commit(context);
	}

	/**
	 * 图的深度遍历操作(递归)
	 * 
	 * @param nodeCount
	 * @param vertexs
	 * @param edges
	 */
	public static void DFSTraverse(int nodeCount, String[] vertexs,
			String[][] edges) {
		flag = new boolean[nodeCount];
		for (int i = 0; i < nodeCount; i++) {
			similar_lables = "";
			if (flag[i] == false) {
				// 当前顶点没有被访问
				DFS(i, nodeCount, vertexs, edges);
				label_set_list.add(similar_lables);
			}
		}
	}

	/**
	 * 图的深度优先递归算法
	 * 
	 * @param i
	 * @param nodeCount
	 * @param vertexs
	 * @param edges
	 */
	public static void DFS(int i, int nodeCount, String[] vertexs,
			String[][] edges) {
		flag[i] = true;// 第i个顶点被访问
		similar_lables += vertexs[i] + " ";
		for (int j = 0; j < nodeCount; j++) {
			if (flag[j] == false && edges[i][j].equalsIgnoreCase("1")) {
				DFS(j, nodeCount, vertexs, edges);
			}
		}
	}

	/**
	 * 计算最长公共子序列
	 * 
	 * @param x
	 * @param y
	 */
	public static void LCSAlgorithm(String[] x, String[] y) {
		feature = "P,";
		int[][] b = getLength(x, y);
		Display(b, x, x.length - 1, y.length - 1);
	}

	public static int[][] getLength(String[] x, String[] y) {
		int[][] b = new int[x.length][y.length];
		int[][] c = new int[x.length][y.length];
		for (int i = 1; i < x.length; i++) {
			for (int j = 1; j < y.length; j++) {
				if (x[i].equals(y[j])) {
					c[i][j] = c[i - 1][j - 1] + 1;
					b[i][j] = 1;
				} else if (c[i - 1][j] >= c[i][j - 1]) {
					c[i][j] = c[i - 1][j];
					b[i][j] = 0;
				} else {
					c[i][j] = c[i][j - 1];
					b[i][j] = -1;
				}
			}
		}
		return b;
	}

	public static void Display(int[][] b, String[] x, int i, int j) {
		if (i == 0 || j == 0)
			return;
		if (b[i][j] == 1) {
			Display(b, x, i - 1, j - 1);
			feature += x[i] + " ";
		} else if (b[i][j] == 0) {
			Display(b, x, i - 1, j);
		} else if (b[i][j] == -1) {
			Display(b, x, i, j - 1);
		}
	}
}
