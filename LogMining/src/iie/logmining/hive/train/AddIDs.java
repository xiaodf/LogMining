package iie.logmining.hive.train;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import iie.udps.common.hcatalog.SerHCatInputFormat;

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

import scala.Tuple2;

/*
 * 从HiveDataProcessing.java处理后得到的hive表里读取message数据
 * 加上id后存回新的hive里，此操作必须在local模式下运行
 * 
 * spark-submit --class iie.logmining.data.train.AddIDs --master local --jars /home/xdf/udps-sdk-0.0.1.jar,/home/xdf/jars/lucene-analyzers-common-4.10.2.jar,/home/xdf/spark-mllib_2.10-1.3.0.jar
 *   /home/xdf/test2.jar -c /user/xdf/stdin.xml
 */
public class AddIDs {

	@SuppressWarnings({ "rawtypes" })
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}
		String stdinXml = args[1];
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(stdinXml);// 参数列表

		String dbName = list.get(0).get("dbName").toString(); // xdf
		// String processingOutTable = list.get(0).get("processingOutTable")
		// .toString(); // 输入表
		// String addIDOutTable = list.get(0).get("addIDOutTable").toString();
		// // 输出表
		String processingOutTable = "logs4"; // 输入表
		String addIDOutTable = "logs3"; // 输出表
		String addIDTabSchema = list.get(0).get("addIDTabSchema").toString(); // 输出表结构
		SparkConf sparkConf = new SparkConf().setAppName("AddIDs");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// 读取表message数据
		JavaRDD<String> result = getMessageData(jsc, dbName, processingOutTable);

		// 调用hive sql生成中间输出表表addIDOutTable保存数据
//		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(
//				jsc.sc());
//		sqlContext.sql("create table " + dbName + "." + addIDOutTable
//				+ "(" + addIDTabSchema + ")");
		// 对每条数据加上唯一ID后存回另一张表中
		storeToTable(result, dbName, addIDOutTable);
		//删除输入表
		//sqlContext.sql("drop table " + dbName + "." + processingOutTable);
		jsc.stop();
		System.exit(0);
	}

	/**
	 * 读取表message数据
	 * 
	 * @param jsc
	 * @param dbName
	 * @param processingOutTable
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	public static JavaRDD<String> getMessageData(JavaSparkContext jsc,
			String dbName, String processingOutTable) throws IOException {
		Configuration inputConf = new Configuration();
		SerHCatInputFormat.setInput(inputConf, dbName, processingOutTable);

		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(inputConf, SerHCatInputFormat.class,
						WritableComparable.class, SerializableWritable.class);

		JavaRDD<String> result = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, String>() {
					private static final long serialVersionUID = -2362812254158054659L;

					public String call(
							Tuple2<WritableComparable, SerializableWritable> v)
							throws Exception {
						HCatRecord record = (HCatRecord) v._2.value();
						return record.get(0).toString();
					}
				});
		return result;
	}

	/**
	 * 将处理后的数据存到输出表中
	 * 
	 * @param rdd
	 * @param dbName
	 * @param addIDOutTable
	 * @throws HCatException
	 */
	public static void storeToTable(JavaRDD<String> rdd, String dbName,
			String addIDOutTable) throws HCatException {
		// 将处理后的数据存到输出表中
		List<String> message = rdd.collect();
		// 生成数据（类别，特征，IDS）存到hive表中
		WriteEntity.Builder builder = new WriteEntity.Builder();
		WriteEntity entity = builder.withDatabase(dbName)
				.withTable(addIDOutTable).build();
		Map<String, String> config = new HashMap<String, String>();
		HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
		WriterContext context = writer.prepareWrite();
		HCatWriter splitWriter = DataTransferFactory.getHCatWriter(context);
		List<HCatRecord> records = new ArrayList<HCatRecord>();
		for (int i = 0; i < message.size(); i++) {
			List<Object> tmp = new ArrayList<Object>(2);
			tmp.add(i);
			tmp.add(message.get(i));
			records.add(new DefaultHCatRecord(tmp));
		}
		splitWriter.write(records.iterator());
		writer.commit(context);
	}
}
