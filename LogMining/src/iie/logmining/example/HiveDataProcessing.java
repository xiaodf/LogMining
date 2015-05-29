package iie.logmining.example;

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
 * spark-submit --class iie.logmining.hive.train.HiveDataProcessing 
 * --master yarn-client /home/xdf/test.jar xdf log6 log7 1 st=\'1\'
 * 
 */
public class HiveDataProcessing {

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
		String sourceTable = list.get(0).get("sourceTable").toString(); // 数据源输入表test1
		String processingOutTable = list.get(0).get("processingOutTable")
				.toString(); // 输出表
		int messageFieldPos = Integer.parseInt(list.get(0)
				.get("messageFieldPos").toString()); // 输入表的message字段位置

		// String partition =list.get(0).get("partition").toString(); //
		SparkConf sparkConf = new SparkConf().setAppName("HiveDataProcessing");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// 读取hive表message字段数据，进行分词，去干扰词，过滤处理，返回RDD类型数据
		JavaRDD<SerializableWritable<HCatRecord>> LastRDD = getProcessedMessage(
				jsc, dbName, sourceTable, messageFieldPos);
		// 将处理后的数据存到输出表中
		storeToTable(LastRDD, dbName, processingOutTable);
		jsc.stop();
		System.exit(0);
	}

	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> getProcessedMessage(
			JavaSparkContext jsc, String dbName, String sourceTable,
			int messageFieldPos) throws IOException {
		// 处理数据
		Configuration inputConf = new Configuration();
		Job job = Job.getInstance(inputConf);
		SerHCatInputFormat.setInput(job, dbName, sourceTable);
		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(job.getConfiguration(),
						SerHCatInputFormat.class, WritableComparable.class,
						SerializableWritable.class);

		final Broadcast<Integer> posBc = jsc.broadcast(messageFieldPos);// 将message字段位置广播出去
		// 获取表记录集
		JavaPairRDD<String, String> messageRDD = rdd
				.mapToPair(new PairFunction<Tuple2<WritableComparable, SerializableWritable>, String, String>() {
					private static final long serialVersionUID = 1L;
					// 去除不符合条件的数据
					final String regHexadecimal = "^[0-9a-fA-Fx]*$";// 16进制
					final String regNumber = "^[0-9.,]*$";// 10进制
					final String regStoreMemory = "(?!^[kmgb]*$)^([0-9kmgb.])*$";// 存储大小，不去除mb，gb
					final String regIP = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
					private final int postion = posBc.getValue().intValue();
					private StandardAnalyzer analyzer;// luncene分词器

					@Override
					public Tuple2<String, String> call(
							Tuple2<WritableComparable, SerializableWritable> arg0)
							throws Exception {
						HCatRecord record = (HCatRecord) arg0._2.value();
						String id = record.get(0).toString();// 日志id
						String message = record.get(postion).toString();// 日志message
						List<String> sw = new LinkedList<String>();
						sw.add("");
						CharArraySet stopWords = new CharArraySet(sw, true);
						// 加入系统默认停用词
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
							if ((!isNumber) && (!isIP) && (!isHexadecimal)
									&& (!isStoreMemory)) {
								reMessage += recordArr.get(j) + ",";
							}
						}
						return new Tuple2<String, String>(reMessage, id);
					}
				});

		// 按key分组合并日志，去重的同时记录下相同key的id号供下一阶段使用
		JavaPairRDD<String, Iterable<String>> result = messageRDD.groupByKey();
		JavaRDD<SerializableWritable<HCatRecord>> LastRDD = result
				.map(new Function<Tuple2<String, Iterable<String>>, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public SerializableWritable<HCatRecord> call(
							Tuple2<String, Iterable<String>> arg0)
							throws Exception {
						HCatRecord hcat = new DefaultHCatRecord(2);
						hcat.set(0, arg0._1);
						Iterator<String> temp = arg0._2.iterator();
						String str = "";
						while (temp.hasNext()) {
							str += temp.next() + ",";
						}
						hcat.set(1, str);
						return new SerializableWritable<HCatRecord>(hcat);
					}
				});
		// 处理数据
		return LastRDD;

	}

	// 将处理后的数据存到输出表中
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
