package iie.logmining.hive.train;

import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

import scala.Tuple2;

/*
 * Spark读hive表里面的message字段数据,进行分词、去重、去干扰词之后存到
 * 另一张hive表里
 * 命令：
 * spark-submit --class iie.logmining.data.train.HiveDataProcessing 
 * --master yarn-cluster --num-executors 40 --driver-memory 512m  --executor-memory 1g  --executor-cores 1
 * --jars /home/xdf/udps-sdk-0.0.1.jar,/home/xdf/jars/lucene-core-4.10.2.jar,/home/xdf/jars/lucene-analyzers-common-4.10.2.jar,/home/xdf/spark-mllib_2.10-1.3.0.jar
 *   /home/xdf/test1.jar -c /user/xdf/stdin.xml

 * 
 */
public class GetMessageAndProcess {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}
		String stdinXml = args[1];
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(stdinXml);// 参数列表

		String dbName = list.get(0).get("dbName").toString(); // 数据库 xdf
		// String sourceTable = list.get(0).get("sourceTable").toString();
		// //数据源输入表test1
		String sourceTable = args[2];
		int messageFieldPos = 3;
		// String processingOutTable = "log2"; // 输出表
		String processingOutTable = list.get(0).get("processingOutTable")
				.toString(); // 输出表
		String processingTabSchema = list.get(0).get("processingTabSchema")
				.toString(); // 输出表结构
		// String sourceTable = "alldata3"; // 数据源输入表test1

		// int messageFieldPos = Integer.parseInt(list.get(0)
		// .get("messageFieldPos").toString()); // 输入表的message字段位置
		// String partition = list.get(0).get("partition").toString(); //
		// String partition = args[2];
		SparkConf sparkConf = new SparkConf().setAppName("HiveDataProcessing");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// 读取hive表message字段数据，进行分词，去干扰词，过滤处理，返回RDD类型数据
		JavaRDD<SerializableWritable<HCatRecord>> LastRDD = getProcessedMessage(
				jsc, dbName, sourceTable, messageFieldPos);

		// 调用hive sql生成中间输出表表processingOutTable保存数据
		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(
				jsc.sc());
		sqlContext.sql("create table " + dbName + "." + processingOutTable
				+ "(" + processingTabSchema + ")");
		// 将处理后的数据存到输出表中
		storeToTable(LastRDD, dbName, processingOutTable);
		jsc.stop();
		System.exit(0);
	}

	/**
	 * 读取hive表message字段数据，进行分词，去干扰词，过滤处理，返回RDD类型数据
	 * 
	 * @param jsc
	 * @param dbName
	 * @param sourceTable
	 * @param messageFieldPos
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> getProcessedMessage(
			JavaSparkContext jsc, String dbName, String sourceTable,
			int messageFieldPos) throws IOException {
		// 处理数据
		Configuration inputConf = new Configuration();
		Job job = Job.getInstance(inputConf);
		SerHCatInputFormat
				.setInput(job.getConfiguration(), dbName, sourceTable);// "col6 >=\'2014-12-01\' and col6 <=\'2014-12-10\'"
		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(job.getConfiguration(),
						SerHCatInputFormat.class, WritableComparable.class,
						SerializableWritable.class);

		final Broadcast<Integer> posBc = jsc.broadcast(messageFieldPos);// 将message字段位置广播出去
		// 获取表记录集
		JavaRDD<String> messageRDD = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, String>() {
					private static final long serialVersionUID = 1L;
					// 去除不符合条件的数据
					final String regHexadecimal = "^[0-9a-fA-Fx]*$";// 16进制
					final String regNumber = "^[0-9.,]*$";// 10进制
					final String regUnderLine = "[^_]*";// 下划线
					final String regNotation = "[\\.]";// 小数点
					final String regStoreMemory = "(?!^[kmgb]*$)^([0-9kmgb.])*$";// 存储大小，不去除mb，gb
					final String regIP = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
					private final int postion = posBc.getValue().intValue();
					private StandardAnalyzer analyzer;// luncene分词器

					@Override
					public String call(
							Tuple2<WritableComparable, SerializableWritable> arg0)
							throws Exception {
						HCatRecord record = (HCatRecord) arg0._2.value();
						String[] tempMessage = record.get(postion).toString()
								.split(";| ");
						String message = "";// 日志message
						// 如果message字符串包含BJLTSH-503-DFA-CL-SEV字段，将此字段删除
						String regSegment = "BJLTSH-503-DFA-CL-SEV\\d+";
						if (tempMessage[0].matches(regSegment)) {
							String tempStr = "";
							for (int k = 1; k < tempMessage.length; k++) {
								tempStr += tempMessage[k] + " ";
							}
							message = tempStr;
						} else {
							message = record.get(postion).toString();
						}
						List<String> sw = new LinkedList<String>();
						sw.add("");
						CharArraySet stopWords = new CharArraySet(sw, true);
						stopWords.add("_");
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
						String reMessage = "";
						for (int j = 0; j < recordArr.size(); j++) {
							boolean isNumber = recordArr.get(j).matches(
									regNumber);
							boolean isIP = recordArr.get(j).matches(regIP);
							boolean isHexadecimal = recordArr.get(j).matches(
									regHexadecimal);
							boolean isStoreMemory = recordArr.get(j).matches(
									regStoreMemory);
							boolean isNotUnderLine = recordArr.get(j).matches(
									regUnderLine);

							if ((!isNumber) && (!isIP) && (!isHexadecimal)
									&& (!isStoreMemory) && (isNotUnderLine)) {

								String finalStr = recordArr.get(j);
								// 去除字符串末尾的数字和标点
								String temp = finalStr.substring(finalStr
										.length() - 1);
								boolean tailIsNumber = temp.matches(regNumber)
										|| temp.matches(regNotation);
								String str = "";
								if (tailIsNumber) {
									for (int i = finalStr.length() - 1; i > 0; i--) {
										String tempStr = finalStr.substring(i);
										boolean isNumber2 = tempStr
												.matches(regNumber)
												|| tempStr.matches(regNotation);
										str = finalStr.substring(0, i + 1);
										if (!isNumber2)
											break;
									}
									if (j < recordArr.size() - 1) {
										reMessage += str + " ";
									}
									if (j == recordArr.size() - 1) {
										reMessage += str;
									}
								} else {
									if (j < recordArr.size() - 1) {
										reMessage += finalStr + " ";
									}
									if (j == recordArr.size() - 1) {
										reMessage += finalStr;
									}
								}
							}
						}
						return reMessage;
					}
				}).distinct();

		// 按key分组合并日志，去重的同时记录下相同key的id号供下一阶段使用
		JavaRDD<SerializableWritable<HCatRecord>> LastRDD = messageRDD
				.map(new Function<String, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public SerializableWritable<HCatRecord> call(String arg0)
							throws Exception {
						HCatRecord hcat = new DefaultHCatRecord(1);
						hcat.set(0, arg0);
						return new SerializableWritable<HCatRecord>(hcat);
					}
				});
		// 返回处理后的数据
		return LastRDD;
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
			outputJob.setJobName("HiveDataProcessing");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			HCatSchema schema = SerHCatOutputFormat.getTableSchemaWithPart(outputJob
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
