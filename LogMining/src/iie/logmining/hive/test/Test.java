package iie.logmining.hive.test;

import java.io.IOException;

import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Test {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws IOException {
    
		String dbName = "xdf";
		String input = "alarm1";
		String output = "alarm_partition";
		
		SparkConf sparkConf = new SparkConf().setAppName("HiveDataProcessing");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// 处理数据
		Configuration inputConf = new Configuration();
		Job job = Job.getInstance(inputConf);
		SerHCatInputFormat
				.setInput(job.getConfiguration(), dbName, input);// "col6 >=\'2014-12-01\' and col6 <=\'2014-12-10\'"
		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(job.getConfiguration(),
						SerHCatInputFormat.class, WritableComparable.class,
						SerializableWritable.class);
		JavaRDD<SerializableWritable<HCatRecord>> result = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = -2362812254158054659L;

					@Override
					public SerializableWritable<HCatRecord> call(
							Tuple2<WritableComparable, SerializableWritable> v)
							throws Exception {
						HCatRecord record = (HCatRecord) v._2.value();
						HCatRecord hcat = new DefaultHCatRecord(5);
						hcat.set(0,Integer.parseInt(record.get(1).toString()));
						hcat.set(1, record.get(0).toString());
						hcat.set(2, record.get(2).toString());
						hcat.set(3, record.get(3).toString());

						String[] time = record.get(0).toString().split(" ");
					    hcat.set(4, time[0]);
						return new SerializableWritable<HCatRecord>(hcat);
					}
				});
		// 将处理后的数据存到输出表中
		storeToTable(result, dbName, output);
		jsc.stop();
		System.exit(0);		
		
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
}
