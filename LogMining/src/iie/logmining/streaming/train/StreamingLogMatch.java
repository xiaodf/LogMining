package iie.logmining.streaming.train;

import iie.kafka.topics.HIVETopicGenerator;
import iie.kafka.topics.Topic;
import iie.kafka.topics.TopicErrorException;
import iie.kafka.topics.TopicNotFoundException;
import iie.logmining.hive.train.OperatorParamXml;
import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/*
 * spark-submit --master yarn-client --class main.LogMatcherFinal 
 * --jars /root/wtj/lib/kafka-topics-1.0-SNAPSHOT.jar,/root/wtj/lib/udps-sdk-0.0.1.jar,/root/wtj/lib/kafka_2.10-0.8.1.1.jar,/root/wtj/lib/spark-streaming-kafka_2.10-1.2.0.jar,/root/wtj/lib/zkclient-0.4.jar,/root/wtj/lib/guava-18.0.jar,/root/wtj/lib/metrics-core-2.2.0.jar,/root/wtj/lib/lucene-core-4.10.2.jar,/root/wtj/lib/lucene-analyzers-common-4.10.2.jar
 *  LogMatcher.jar /root/wtj/test.properties /root/wtj/checkpoint/
 */

public class StreamingLogMatch {

	public static double DEFAULT_MATCH_RATE = 0.0;
	public static List<Tuple2<String, Integer>> TOPIC_LIST = new ArrayList<Tuple2<String, Integer>>();

	public static String ZK_QUORUM = null;
	public static String GROUP_ID = null;

	public static String INPUT_DB_NAME = null;
	public static String INPUT_FEATURE_TABLE = null;
	public static int SELECT_MATCH_FIELD = 0;
	public static int SELECT_TIME_FIELD = 0;

	public static String OUTPUT_DB_NAME = null;
	public static String OUTPUT_MATCHED_TABLE = null;
	public static String OUTPUT_WARNING_TABLE = null;
	public static String OUTPUT_NOTAG_TABLE = null;

	public static String THRIFT_URIS = null;
	public static String CHECKPOINT_PATH = null;
	public static String SPARK_STREAMING_DURATION = null;
	public static String TOPIC_Name = null;

	/**
	 * 以下取特征库RDD方法不再更改，对这个RDD进行Repartion(1).collection()返回此RDD内数据构成的Map
	 * 再对Map进行处理
	 */
	// 从Hive表中取出训练好的特征库到RDD中，以备处理
	@SuppressWarnings({ "rawtypes" })
	private static JavaRDD<SerializableWritable> getFeatureLibraryRDD(
			JavaSparkContext jsc, String dbName, String inputTabName)
			throws IOException {

		Configuration inputConf = new Configuration();
		Job job = Job.getInstance(inputConf);
		SerHCatInputFormat.setInput(job.getConfiguration(), dbName,
				inputTabName);
		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(job.getConfiguration(),
						SerHCatInputFormat.class, WritableComparable.class,
						SerializableWritable.class);

		@SuppressWarnings("serial")
		JavaRDD<SerializableWritable> rdd2 = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable>() {
					public SerializableWritable call(
							Tuple2<WritableComparable, SerializableWritable> v) {
						return v._2;
					}
				});
		return rdd2;
	}

	/**
	 * 首先根据特征库RDD转换出preFeatureMap准备处理，迭代去除preFeatureMap中的每条记录，
	 * 创建最终的倒排索引特征表finalFeatureMap用于日志匹配
	 */
	@SuppressWarnings("rawtypes")
	private static Map<String, Map<String, Double>> getFinalFeatureMap(
			List<SerializableWritable> preFeatureList) {

		Map<String, Map<String, Double>> finalFeatureMap = new HashMap<String, Map<String, Double>>();

		Iterator<SerializableWritable> read = preFeatureList.iterator();

		while (read.hasNext()) {
			SerializableWritable current = read.next();
			HCatRecord record = (HCatRecord) current.value();
			String key = record.get(0).toString();// 类别
			String[] word = record.get(1).toString().split(" ");// 特征

			for (int j = 0; j < word.length; j++) {
				if (!finalFeatureMap.containsKey(word[j])) {
					Map<String, Double> wordMap = new HashMap<String, Double>();
					wordMap.put(key, 1.0);
					finalFeatureMap.put(word[j], wordMap);
				} else {
					if (!finalFeatureMap.get(word[j]).containsKey(key)) {
						finalFeatureMap.get(word[j]).put(key, 1.0);
					} else {
						double value = finalFeatureMap.get(word[j]).get(key) + 1;
						finalFeatureMap.get(word[j]).put(key, value);
					}
				}
			}
		}
		return finalFeatureMap;
	};

	/**
	 * 方法实现同getFinalFeatureMap()，再次遍历preFeatureMap统计每个Tag下包含特征词的数量，用于计算日志匹配率
	 */
	@SuppressWarnings("rawtypes")
	private static Map<String, Double> getFinalFeatureCountMap(
			List<SerializableWritable> preFeatureList) {

		Map<String, Double> finalFeatureCountMap = new HashMap<String, Double>();

		Iterator<SerializableWritable> read = preFeatureList.iterator();

		while (read.hasNext()) {
			SerializableWritable current = read.next();
			HCatRecord record = (HCatRecord) current.value();
			String key = record.get(0).toString();
			String[] word = record.get(1).toString().split(" ");
			finalFeatureCountMap.put(key, (double) word.length);
		}
		return finalFeatureCountMap;

	};

	/**
	 * 对参数赋初始值，仅用于测试，再读取配置文件功能完善之后不再使用
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void setProperties(String stdinXml, String groupid, String topics)
			throws Exception {
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(stdinXml);// 参数列表

		INPUT_DB_NAME = list.get(0).get("dbName").toString(); // 输入数据库名
		// INPUT_FEATURE_TABLE = list.get(0).get("featureTable").toString(); //
		// 输入训练阶段生成的特征数据表名
		INPUT_FEATURE_TABLE = "logs4";
		DEFAULT_MATCH_RATE = Double.parseDouble(list.get(0).get("matchRate")
				.toString()); // 默认测试日志数据与特征数据相似率
		// SELECT_MATCH_FIELD = Integer.parseInt(list.get(0)
		// .get("logMessageField").toString());
		SELECT_MATCH_FIELD = 3;
		SELECT_TIME_FIELD = 1;
		ZK_QUORUM = list.get(0).get("zkquorum").toString();
		// GROUP_ID = list.get(0).get("groupID").toString();
		GROUP_ID = groupid;
		THRIFT_URIS = list.get(0).get("thriftURI").toString();
		OUTPUT_DB_NAME = list.get(0).get("outputDBName").toString();
		// OUTPUT_MATCHED_TABLE =
		// list.get(0).get("outputMatchTable").toString(); // log2
		// OUTPUT_WARNING_TABLE =
		// list.get(0).get("outputWarningTable").toString();
		// OUTPUT_NOTAG_TABLE = list.get(0).get("outputNotagTable").toString();
		OUTPUT_MATCHED_TABLE = "labelmatch2"; // log2
		OUTPUT_WARNING_TABLE = "warningmatch2";
		OUTPUT_NOTAG_TABLE = "notagmatch2";
		List<String> topicAndThreads = (List<String>) list.get(2).get(
				"topicAndThreads");
		for (int i = 0; i < topicAndThreads.size(); i++) {
			String[] topic = topicAndThreads.get(i).toString().split(":");
			 TOPIC_LIST.add(new Tuple2<String, Integer>(topics, Integer
			 .parseInt(topic[1])));
			//TOPIC_LIST.add(new Tuple2<String, Integer>(topics, 20));

		}
	}

	@SuppressWarnings("rawtypes")
	public static void storeToTable(
			JavaRDD<SerializableWritable<HCatRecord>> rdd, String dbName,
			String tblName) {
		Job outputJob = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("SaveToHiveTable");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			HCatSchema schema = SerHCatOutputFormat
					.getTableSchemaWithPart(outputJob.getConfiguration());
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

	/**
	 * 调用lucene的分词器对原始字符串进行分词操作，返回分词之后的List<String>
	 * 
	 */
	public static List<String> luceneSplitLine(String originalLine)
			throws IOException {

		// 此处开始调用lucene分词器
		List<String> sw = new LinkedList<String>();
		sw.add("");
		CharArraySet stopWords = new CharArraySet(sw, true);
		// 加入系统默认停用词
		Iterator<Object> iter = StandardAnalyzer.STOP_WORDS_SET.iterator();
		while (iter.hasNext()) {
			stopWords.add(iter.next());
		}
		// 标准分词器(Lucene内置的标准分析器,会将语汇单元转成小写形式，并去除停用词及标点符号)
		@SuppressWarnings("resource")
		StandardAnalyzer analyzer = new StandardAnalyzer(stopWords);
		TokenStream ts = analyzer.tokenStream("field", originalLine);
		CharTermAttribute ch = ts.addAttribute(CharTermAttribute.class);
		ts.reset();
		List<String> splitLine = new ArrayList<String>();
		final String regHexadecimal = "^[0-9a-fA-Fx]*$";// 16进制
		final String regNumber = "^[0-9.,]*$";// 10进制
		final String regUnderLine = "[^_]*";// 下划线
		final String regNotation = "[\\.]";// 小数点
		final String regStoreMemory = "(?!^[kmgb]*$)^([0-9kmgb.])*$";// 存储大小，不去除mb，gb
		final String regIP = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
		while (ts.incrementToken()) {
			String word = ch.toString();
			boolean isNumber = word.matches(regNumber);
			boolean isIP = word.matches(regIP);
			boolean isHexadecimal = word.matches(regHexadecimal);
			boolean isStoreMemory = word.matches(regStoreMemory);
			boolean isNotUnderLine = word.matches(
					regUnderLine);
			if ((!isNumber) && (!isIP) && (!isHexadecimal) && (!isStoreMemory)&& (isNotUnderLine)) {
				String finalStr = word;
				// 去除字符串末尾的数字
				String temp = finalStr.substring(finalStr.length() - 1);
				// 判断字符串最后一位是否是数字或点号
				boolean tailIsNumber = temp.matches(regNumber)
						|| temp.matches(regNotation);
				String str = "";// 返回处理后的字符串
				if (tailIsNumber) {
					for (int i = finalStr.length() - 1; i > 0; i--) {
						String tempStr = finalStr.substring(i);
						boolean isNumber2 = tempStr.matches(regNumber)
								|| tempStr.matches(regNotation);
						str = finalStr.substring(0, i + 1);
						if (!isNumber2)
							break;
					}
					splitLine.add(str);
				} else {
					// 若字符串末尾不是数据，直接保存
					splitLine.add(finalStr);
				}
			}
		}
		ts.end();
		ts.close();
		return splitLine;
	}

	@SuppressWarnings({ "rawtypes", "serial" })
	public static void main(String args[]) throws Exception {

		if (args.length < 3) {
			System.err.println("Usage: <-c> <stdin.xml> <checkpointPath>");
			System.exit(1);
		}
		String stdinXml = args[1];
		String port = args[3];
		String topics = args[4];

		// 读取配置文件，设置参数
		setProperties(stdinXml, port, topics);

		// 创建程序入口相关对象
		SparkConf conf = new SparkConf();
		conf.setAppName("StreamingLogMatcher");
		conf.set("spark.streaming.receiver.writeAheadLog.enable", "true");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(
				5000));
		jssc.checkpoint(args[2]);

		JavaRDD<SerializableWritable> featureRDD = null;
		try {
			// 获取特征
			featureRDD = getFeatureLibraryRDD(jsc, INPUT_DB_NAME,
					INPUT_FEATURE_TABLE);
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<SerializableWritable> preFeatureList = featureRDD.repartition(1)
				.collect();

		Map<String, Map<String, Double>> finalFeatureMap = getFinalFeatureMap(preFeatureList);
		Map<String, Double> finalFeatureCountMap = getFinalFeatureCountMap(preFeatureList);

		// 将两个特征表进行广播用于流计算
		final Broadcast<Map<String, Map<String, Double>>> broadcastFeatureMap = jsc
				.broadcast(finalFeatureMap);
		final Broadcast<Map<String, Double>> broadcastFeatureCountMap = jsc
				.broadcast(finalFeatureCountMap);
		final Broadcast<Double> broadcastDefaultMatchRate = jsc
				.broadcast(DEFAULT_MATCH_RATE);
		final Broadcast<Integer> messageField = jsc
				.broadcast(SELECT_MATCH_FIELD);
		final Broadcast<Integer> timeField = jsc.broadcast(SELECT_TIME_FIELD);
		/**
		 * 以下进入SparkStreaming日志匹配部分
		 */
		Map<String, Integer> topicMap = new HashMap<String, Integer>();

		topicMap.clear();
		// 获取（topic,线程数）
		topicMap.put(TOPIC_LIST.get(0)._1, TOPIC_LIST.get(0)._2);
		Topic topic = null;
		try {
			topic = new HIVETopicGenerator(THRIFT_URIS).getTopic(TOPIC_LIST
					.get(0)._1);
		} catch (TopicErrorException e) {
			e.printStackTrace();
		} catch (TopicNotFoundException e) {
			e.printStackTrace();
		}
		String schema = topic.getSchema().toString();
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("zookeeper.connect", ZK_QUORUM);
		kafkaParams.put("group.id", GROUP_ID);
		kafkaParams.put("zookeeper.connection.timeout.ms", "10000");
		kafkaParams.put("KafkaTopicSchema", schema);
		kafkaParams.put("auto.offset.reset", "smallest");// topic数据可以重复读取

		JavaPairInputDStream<String, String[]> eachStream = KafkaUtils
				.createStream(jssc, String.class, String[].class,
						StringDecoder.class, AvroSDecoder.class, kafkaParams,
						topicMap, StorageLevel.MEMORY_AND_DISK_SER());

		JavaDStream<SerializableWritable<HCatRecord>> finalResultStream = eachStream
				.map(new Function<Tuple2<String, String[]>, SerializableWritable<HCatRecord>>() {

					int mesField = messageField.getValue();
					int dayField = timeField.getValue();

					public SerializableWritable<HCatRecord> call(
							Tuple2<String, String[]> message)
							throws IOException {

						// 将Kafka反序列化得到的字符串数组保存到currentLine中
						String[] currentLine = message._2;
						String TAG = "";
						// 获取message数据
						String currentLineSeg = currentLine[mesField];
						// 如果message数据包含BJLTSH-503-DFA-CL-SEV字段，将此字段删除
						String regSegment = "BJLTSH-503-DFA-CL-SEV\\d+";
						String[] tempMessage = currentLineSeg.split(";| ");
						String currentMessage = "";// 日志message
						if (tempMessage[0].matches(regSegment)) {
							String tempStr = "";
							for (int k = 1; k < tempMessage.length; k++) {
								tempStr += tempMessage[k] + " ";
							}
							currentMessage = tempStr;
						} else {
							currentMessage = currentLineSeg;
						}
						// 调用lucene对处理后的message进行分词、去干扰词
						List<String> splitLine = luceneSplitLine(currentMessage);
						// 创建相关哈希表和读取当前行后
						// 一、首先获取特征词命中数哈希表numMap
						// 将当前读取行分词后，每个词与特征表匹配
						Map<String, Double> numMap = new HashMap<String, Double>();
						Map<String, Double> rateMap = new HashMap<String, Double>();

						for (int i = 0; i < splitLine.size(); i++) {
							String currentKey = splitLine.get(i);
							if (broadcastFeatureMap.getValue().containsKey(
									currentKey)) {
								// 将该词在特征表中所对应的标签存入numMap中标记命中次数
								Set<String> tagSet = broadcastFeatureMap
										.getValue().get(currentKey).keySet();
								// 将词对应的标签放入迭代器中迭代取值
								for (Iterator<String> iterator = tagSet
										.iterator(); iterator.hasNext();) {
									String tag = iterator.next();
									// 若numMap中没有当前标签信息，在表中加入此标签且命中数置一
									if (!numMap.containsKey(tag)) {
										numMap.put(tag, 1.0);
									}
									// 若numMap中已有当前标签，则将当前标签命中数加一
									else {
										numMap.put(tag, numMap.get(tag) + 1.0);
									}
								}
							} else {
								// 特征表中不含当前词，继续循环，以后可能添加其他计算过程
							}
						}

						// ------------------------------------------------------------------------
						Set<String> kSet = numMap.keySet();
						// 二、根据命中数表计算命中率表rateMap
						for (Iterator<String> kiterator = kSet.iterator(); kiterator
								.hasNext();) {
							String key = kiterator.next();
							rateMap.put(key,
									numMap.get(key)
											/ broadcastFeatureCountMap
													.getValue().get(key));
						}

						// 创建一个List，用于保存匹配到的0个或多个Tag
						List<String> finalTag = new ArrayList<String>();
						double maxMatchNum = 0;
						for (Iterator<String> kiterator = kSet.iterator(); kiterator
								.hasNext();) {
							String key = kiterator.next();
							if (numMap.get(key) > maxMatchNum) {
								finalTag.clear();
								maxMatchNum = numMap.get(key);
								finalTag.add(key);
							} else if (numMap.get(key) == maxMatchNum) {
								finalTag.add(key);
							} else {
								// 当前值小于maxMatchNum，跳过
							}
						}
						// ------------------------------------------------------------------------
						// 根据finalTag中所包含的元素数，决定返回的Tag类型
						List<String> finalTagWithRate = new ArrayList<String>();
						if (finalTag.size() > 1) {
							double maxMatchRate = 0;
							for (int k = 0; k < finalTag.size(); k++) {
								if (rateMap.get(finalTag.get(k)) > maxMatchRate) {
									finalTagWithRate.clear();
									maxMatchRate = rateMap.get(finalTag.get(k));
									finalTagWithRate.add(finalTag.get(k));
								} else if (rateMap.get(finalTag.get(k)) == maxMatchRate) {
									finalTagWithRate.add(finalTag.get(k));
								} else {
									// 当前值小于maxMatchRate，跳过
								}
							}
							if (finalTagWithRate.size() > 1
									&& maxMatchRate >= broadcastDefaultMatchRate
											.getValue()) {
								TAG = "Warning";
							} else if (finalTagWithRate.size() == 1
									&& maxMatchRate >= broadcastDefaultMatchRate
											.getValue()) {
								TAG = finalTagWithRate.get(0);
							} else {
								TAG = "NoTag";
							}

						} else if (finalTag.size() == 1) {
							if (rateMap.get(finalTag.get(0)) >= broadcastDefaultMatchRate
									.getValue()) {
								finalTagWithRate.clear();
								finalTagWithRate.add(finalTag.get(0));
								TAG = finalTagWithRate.get(0);
							} else {
								TAG = "NoTag";
							}
						} else {
							TAG = "NoTag";
						}

						// ------------------------------------------------------------------------

						// hcat字段为原字段数加2,加一个label列和时间分区列
						HCatRecord hcat = new DefaultHCatRecord(
								currentLine.length + 2);
						hcat.set(0, TAG);
						for (int i = 0; i < currentLine.length; i++) {
							if (i == 0) {
								// 第二列为整型
								hcat.set(i + 1,
										Integer.parseInt(currentLine[i]));
							} else {
								hcat.set(i + 1, currentLine[i]);
							}
						}
						// 截取时间的天,加入分区字段
						String[] time = currentLine[dayField].split(" ");
						hcat.set(currentLine.length + 1, time[0]);
						return new SerializableWritable<HCatRecord>(hcat);
					}
				});

		JavaDStream<SerializableWritable<HCatRecord>> warningStream = finalResultStream
				.filter(new Function<SerializableWritable<HCatRecord>, Boolean>() {
					public Boolean call(SerializableWritable<HCatRecord> record) {
						if (record.value().get(0).equals("Warning"))
							return true;
						else
							return false;
					}
				});

		JavaDStream<SerializableWritable<HCatRecord>> notagStream = finalResultStream
				.filter(new Function<SerializableWritable<HCatRecord>, Boolean>() {
					public Boolean call(SerializableWritable<HCatRecord> record) {
						if (record.value().get(0).equals("NoTag"))
							return true;
						else
							return false;
					}
				});

		JavaDStream<SerializableWritable<HCatRecord>> labelStream = finalResultStream
				.filter(new Function<SerializableWritable<HCatRecord>, Boolean>() {
					public Boolean call(SerializableWritable<HCatRecord> record) {
						if (!record.value().get(0).equals("Warning")
								&& !record.value().get(0).equals("NoTag"))
							return true;
						else
							return false;
					}
				});
		warningStream
				.foreachRDD(new Function<JavaRDD<SerializableWritable<HCatRecord>>, Void>() {

					public Void call(
							JavaRDD<SerializableWritable<HCatRecord>> rdd) {

						storeToTable(rdd, OUTPUT_DB_NAME, OUTPUT_WARNING_TABLE);
						return null;

					}
				});
		notagStream
				.foreachRDD(new Function<JavaRDD<SerializableWritable<HCatRecord>>, Void>() {

					public Void call(
							JavaRDD<SerializableWritable<HCatRecord>> rdd) {

						storeToTable(rdd, OUTPUT_DB_NAME, OUTPUT_NOTAG_TABLE);
						return null;

					}
				});
		labelStream
				.foreachRDD(new Function<JavaRDD<SerializableWritable<HCatRecord>>, Void>() {

					public Void call(
							JavaRDD<SerializableWritable<HCatRecord>> rdd) {
						storeToTable(rdd, OUTPUT_DB_NAME, OUTPUT_MATCHED_TABLE);
						return null;
					}
				});

		jssc.start();
		jssc.awaitTermination();

	}

}
