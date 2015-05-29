package iie.logmining.hive.test;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
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

import scala.Tuple2;

public class Test2 {

	public static void main(String[] args) throws Exception {

		String dbName = "xdf"; // 数据库 xdf
		String sourceTable = "alarm1"; // 数据源输入表test1
		String processingOutTable = "alarm";
		SparkConf sparkConf = new SparkConf().setAppName("HiveDataProcessing");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// 读取hive表message字段数据，进行分词，去干扰词，过滤处理，返回RDD类型数据
		// 处理数据
		JavaRDD<SerializableWritable<HCatRecord>> LastRDD = getProcessedMessage(
				jsc, dbName, sourceTable);
		// 将处理后的数据存到输出表中
		storeToTable(LastRDD, dbName, processingOutTable);
		jsc.stop();
		System.exit(0);
	}

	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> getProcessedMessage(
			JavaSparkContext jsc, String dbName, String sourceTable)
			throws IOException {
		// 处理数据
		Configuration inputConf = new Configuration();
		Job job = Job.getInstance(inputConf);
		SerHCatInputFormat
				.setInput(job.getConfiguration(), dbName, sourceTable);
		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(job.getConfiguration(),
						SerHCatInputFormat.class, WritableComparable.class,
						SerializableWritable.class);

		// 获取表记录集
		JavaRDD<SerializableWritable<HCatRecord>> LastRDD = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public SerializableWritable<HCatRecord> call(
							Tuple2<WritableComparable, SerializableWritable> arg0)
							throws Exception {
						HCatRecord record = (HCatRecord) arg0._2.value();
						HCatRecord hcat = new DefaultHCatRecord(record.size()+1);
						for(int i=0;i<record.size();i++){
							hcat.set(i, record.get(i).toString());
						}

						String[] time = record.get(0).toString().split(" ");
						hcat.set(record.size(), time[0]);
						return new SerializableWritable<HCatRecord>(hcat);
					}
				});
		// 处理数据
		return LastRDD;

	}

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
			OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName, tblName,
					null);
			SerHCatOutputFormat.setOutput(outputJob, outputJobInfo);
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
}
