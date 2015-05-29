package iie.logmining.hive.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import iie.logmining.hive.train.OperatorParamXml;
import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/*
 * 源数据和类别数据结合生成类别源数据存到新表中
 * spark-submit --class iie.logmining.data.train.AddLabel --master yarn-client --num-executors 40 --driver-memory 512m  --executor-memory 1g  --executor-cores 1
 * --jars /home/xdf/udps-sdk-0.0.1.jar,/home/xdf/jars/lucene-core-4.10.2.jar,/home/xdf/jars/lucene-analyzers-common-4.10.2.jar,/home/xdf/spark-mllib_2.10-1.3.0.jar
 * /home/xdf/test4.jar -c /user/xdf/stdin.xml
 */
public class AddLabel {

	@SuppressWarnings({ "rawtypes" })
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}
		String stdinXml = args[1];
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(stdinXml);// 参数列表

		String dbName = list.get(0).get("dbName").toString(); // 数据库
		String sourceTable = list.get(0).get("sourceTable").toString(); // 数据源输入表
		String featureTable = list.get(0).get("featureTable").toString(); // 训练阶段生成的特征表
		String labelOutTable = list.get(0).get("labelOutTable").toString();// 加上类别的数据输出表
		int messageFieldPos = Integer.parseInt(list.get(0)
				.get("messageFieldPos").toString()); // 输入表的message字段位置
		int ipFieldPos = Integer.parseInt(list.get(0).get("ipFieldPos")
				.toString()); // 输入表的message字段位置
		int timeFieldPos = Integer.parseInt(list.get(0).get("timeFieldPos")
				.toString()); // 输入表的message字段位置

		// String sourceTable = "alldata3"; // 数据源输入表
		// String featureTable = "logs44"; // 训练阶段生成的特征表
		// String labelOutTable = "logs55"; // 加上类别的数据输出表

		// 创建程序入口相关对象，SparkConf、JavaSparkContext
		SparkConf sparkConf = new SparkConf().setAppName("AddLabel");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// 获得label表里的数据并广播到集群每个节点
		HashMap<String, String> labelAndMessage = getLabelAndMessage(dbName,
				featureTable);
		final Broadcast<HashMap<String, String>> broadcastLabelAndMessage = jsc
				.broadcast(labelAndMessage);

		// 获得源数据表里的数据,与特征标签库里的特征匹配后加上相应类别返回
		JavaRDD<SerializableWritable<HCatRecord>> messageRDD = addLabelToMessage(
				jsc, dbName, sourceTable, broadcastLabelAndMessage,
				messageFieldPos, ipFieldPos, timeFieldPos);

		// 将处理后的数据存到输出表中
		storeToTable(messageRDD, dbName, labelOutTable);
		jsc.stop();
		System.exit(0);
	}

	/**
	 * 获得源数据表里的数据,与特征标签库里的特征匹配后加上相应类别返回
	 * 
	 * @param jsc
	 * @param dbName
	 * @param sourceTable
	 * @param broadcastLabelAndMessage
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> addLabelToMessage(
			JavaSparkContext jsc, String dbName, String sourceTable,
			final Broadcast<HashMap<String, String>> broadcastLabelAndMessage,
			int messageFieldPos, final int ipFieldPos, final int timeFieldPos)
			throws IOException {
		Configuration inputConf = new Configuration();
		Job job = Job.getInstance(inputConf);
		SerHCatInputFormat.setInput(job.getConfiguration(), dbName, sourceTable);
		JavaPairRDD<WritableComparable, SerializableWritable> sourceRDD = jsc
				.newAPIHadoopRDD(job.getConfiguration(),
						SerHCatInputFormat.class, WritableComparable.class,
						SerializableWritable.class);
		final Broadcast<Integer> messagePosBc = jsc.broadcast(messageFieldPos);// 将message字段位置广播出去
		final Broadcast<Integer> ipPosBc = jsc.broadcast(ipFieldPos);// 将message字段位置广播出去
		final Broadcast<Integer> timePosBc = jsc.broadcast(timeFieldPos);// 将message字段位置广播出去
		// 获取表记录集
		JavaRDD<SerializableWritable<HCatRecord>> messageRDD = sourceRDD
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = 1L;
					// 去除不符合条件的数据
					final String regHexadecimal = "^[0-9a-fA-Fx]*$";// 16进制
					final String regNumber = "^[0-9.,]*$";// 10进制
					// final String regUnderLine = "[^_]*";// 下划线
					final String regStoreMemory = "(?!^[kmgb]*$)^([0-9kmgb.])*$";// 存储大小，不去除mb，gb
					final String regIP = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
					private StandardAnalyzer analyzer;// luncene分词器
					HashMap<String, String> labelAndMessageMap = broadcastLabelAndMessage
							.getValue();
					private final int messagePos = messagePosBc.getValue()
							.intValue();
					private final int ipPos = ipPosBc.getValue().intValue();
					private final int timePos = timePosBc.getValue().intValue();

					@Override
					public SerializableWritable<HCatRecord> call(
							Tuple2<WritableComparable, SerializableWritable> arg0)
							throws Exception {
						HCatRecord record = (HCatRecord) arg0._2.value();
						String message = record.get(messagePos).toString();// 日志message
						List<String> sw = new LinkedList<String>();
						sw.add("");
						CharArraySet stopWords = new CharArraySet(sw, true);
						// 加入系统默认停用词SimpleAnalyzer
						Iterator<Object> itor = StandardAnalyzer.STOP_WORDS_SET
								.iterator();
						while (itor.hasNext()) {
							stopWords.add(itor.next());
						}
						analyzer = new StandardAnalyzer(stopWords);
						TokenStream ts = analyzer.tokenStream("field", message);
						CharTermAttribute ch = ts
								.addAttribute(CharTermAttribute.class);
						ts.reset();
						List<String> recordArr = new ArrayList<String>();
						while (ts.incrementToken()) {
							recordArr.add(ch.toString());
						}
						ts.end();
						ts.close();
						// 返回分词过滤后的message
						List<String> reMessage = new ArrayList<String>();

						for (int j = 0; j < recordArr.size(); j++) {
							boolean isNumber = recordArr.get(j).matches(
									regNumber);
							boolean isIP = recordArr.get(j).matches(regIP);
							boolean isHexadecimal = recordArr.get(j).matches(
									regHexadecimal);
							boolean isStoreMemory = recordArr.get(j).matches(
									regStoreMemory);
							// boolean isNotUnderLine =
							// recordArr.get(j).matches(
							// regUnderLine); && (isNotUnderLine)
							if ((!isNumber) && (!isIP) && (!isHexadecimal)
									&& (!isStoreMemory)) {
								reMessage.add(recordArr.get(j));
							}
						}
						if (reMessage.size() == 0) {
							HCatRecord hcat = new DefaultHCatRecord(record
									.size() + 1);
							hcat.set(0,
									Integer.parseInt((String) record.get(0)));
							hcat.set(1, "notag");
							hcat.set(2, record.get(1));
							hcat.set(3, record.get(4));
							hcat.set(4, record.get(3));
							return new SerializableWritable<HCatRecord>(hcat);
						}
						int featureLength = 0;
						String featureLabel = "";
						// 判断分词后的message与哪一个特征最像
						for (Entry<String, String> kv : labelAndMessageMap
								.entrySet()) {
							String[] feature = kv.getKey().split(",");
							// 计算最长公共子序列长度
							int[][] c = new int[reMessage.size()][feature.length];// c[][]存储x、yLCS长度
							for (int p = 1; p < reMessage.size(); p++) {
								for (int q = 1; q < feature.length; q++) {
									if (reMessage.get(p).equals(feature[q])) {
										c[p][q] = c[p - 1][q - 1] + 1;
									} else if (c[p - 1][q] >= c[p][q - 1]) {
										c[p][q] = c[p - 1][q];
									} else {
										c[p][q] = c[p][q - 1];
									}
								}
							}
							// 判断最长公共子序列长度是否符合条件
							int lcsLength = c[reMessage.size() - 1][feature.length - 1] + 1;
							if (lcsLength == feature.length) {
								if (featureLength < lcsLength) {
									featureLength = lcsLength;// 保存相似的特征中长度最长的
									featureLabel = kv.getValue();
								}
							}
						}

						// 对原日志数据加类别后返回
						HCatRecord hcat = new DefaultHCatRecord(
								record.size() + 1);
						hcat.set(0, Integer.parseInt((String) record.get(0)));
						hcat.set(1, featureLabel);
						hcat.set(2, record.get(timePos));
						hcat.set(3, record.get(ipPos));
						hcat.set(4, record.get(messagePos));
						return new SerializableWritable<HCatRecord>(hcat);
					}
				});
		return messageRDD;
	}

	/**
	 * 获得label表里的数据并广播到集群每个节点
	 * 
	 * @param dbName
	 * @param featureTable
	 * @return
	 * @throws HCatException
	 */
	public static HashMap<String, String> getLabelAndMessage(String dbName,
			String featureTable) throws HCatException {
		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = builder.withDatabase(dbName)
				.withTable(featureTable).build();
		Map<String, String> config = new HashMap<String, String>();
		HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
		ReaderContext cntxt = reader.prepareRead();
		final HashMap<String, String> labelAndMessage = new HashMap<String, String>();
		for (int i = 0; i < cntxt.numSplits(); ++i) {
			HCatReader splitReader = DataTransferFactory
					.getHCatReader(cntxt, i);
			Iterator<HCatRecord> itr1 = splitReader.read();
			while (itr1.hasNext()) {
				HCatRecord record = itr1.next();
				// 生成（message,label）键值对
				labelAndMessage.put(record.get(1).toString(), record.get(0)
						.toString());
			}
		}
		return labelAndMessage;
	}

	/**
	 * 将处理后的数据存到输出表中
	 * 
	 * @param rdd
	 * @param dbName
	 * @param tblName
	 */
	@SuppressWarnings("rawtypes")
	public static void storeToTable(
			JavaRDD<SerializableWritable<HCatRecord>> rdd, String dbName,
			String tblName) {
		Job outputJob = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("AddLabelToData");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			HCatSchema schema = SerHCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
			SerHCatOutputFormat.setSchema(outputJob, schema);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 将RDD存储到目标表中
		rdd.mapToPair(
				new PairFunction<SerializableWritable<HCatRecord>, WritableComparable, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = -4658431554556766962L;

					@Override
					public Tuple2<WritableComparable, SerializableWritable<HCatRecord>> call(
							SerializableWritable<HCatRecord> record)
							throws Exception {
						return new Tuple2<WritableComparable, SerializableWritable<HCatRecord>>(
								NullWritable.get(), record);
					}
				}).saveAsNewAPIHadoopDataset(outputJob.getConfiguration());
	}
}
