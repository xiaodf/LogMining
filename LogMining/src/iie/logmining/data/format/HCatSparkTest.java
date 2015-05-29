package iie.logmining.data.format;

import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * spark+hcatalog操作hive表，将表中数据复制到另一张相同结构的表中
 * 
 * CREATE TABLE student_2 (name STRING，age INT) ROW FORMAT DELIMITED FIELDS
 * TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
 *
 * @author xiaodongfang
 *
 */
public class HCatSparkTest {

	public static void main(String[] args) throws IOException {

		String dbName = "xdf";
		String inputTable = "/user/xdf/alarm.txt";
		String outputTable = "alarm";

		JavaSparkContext jsc = new JavaSparkContext(
				new SparkConf().setAppName("HCatSparkTest"));
		JavaRDD<SerializableWritable<HCatRecord>> rdd = jsc
				.textFile(inputTable)
				.map(new Function<String, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public SerializableWritable<HCatRecord> call(String s)
							throws Exception {
						String[] array = s.split(" |\t");
						HCatRecord hcat = new DefaultHCatRecord(
								array.length - 1);
						hcat.set(0, array[0] + " " + array[1]);
						hcat.set(1, array[2]);
						hcat.set(2, array[3]);
						hcat.set(3, array[4]);
						return new SerializableWritable<HCatRecord>(hcat);
					}

				});
		// JavaRDD<SerializableWritable<HCatRecord>> rdd =
		// HCatSparkTest.getData(
		// jsc, dbName, inputTable);
		// rdd.map(new Function<SerializableWritable<HCatRecord>,String>(){
		// private static final long serialVersionUID = 1L;
		// @Override
		// public String call(SerializableWritable<HCatRecord> arg0)
		// throws Exception {
		// HCatRecord hcat = arg0.value();
		// String str = "";
		// for(int i = 0;i<hcat.size() - 1;i++){
		// str += hcat.get(i).toString() + ";";
		// }
		// str += hcat.get(hcat.size() - 1);
		// return str;
		// }
		// }).saveAsTextFile(outputTable);
		HCatSparkTest.store(rdd, dbName, outputTable);
		jsc.stop();
		System.exit(0);
	}

	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> getData(
			JavaSparkContext jsc, String dbName, String tblName)
			throws IOException {

		Configuration inputConf = new Configuration();
		SerHCatInputFormat.setInput(inputConf, dbName, tblName);

		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(inputConf, SerHCatInputFormat.class,
						WritableComparable.class, SerializableWritable.class);

		JavaPairRDD<WritableComparable, SerializableWritable> filter = rdd
				.filter(new Function<Tuple2<WritableComparable, SerializableWritable>, Boolean>() {
					private static final long serialVersionUID = 1L;

					public Boolean call(
							Tuple2<WritableComparable, SerializableWritable> v)
							throws Exception {
						HCatRecord record = (HCatRecord) v._2.value();
						String str = record.get(1).toString().split(",")[0];
						Pattern pattern = Pattern
								.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
						Matcher matcher = pattern.matcher(str.split(" ")[0]);
						boolean bool = matcher.matches();
						return bool;
					}
				});

		// 获取表记录集
		JavaRDD<SerializableWritable<HCatRecord>> result = filter
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable<HCatRecord>>() {

					private static final long serialVersionUID = -2362812254158054659L;

					public SerializableWritable<HCatRecord> call(
							Tuple2<WritableComparable, SerializableWritable> v)
							throws Exception {
						HCatRecord record = (HCatRecord) v._2.value();

						HCatRecord hcat = new DefaultHCatRecord(record.size());
						for (int i = 0; i < record.size(); i++) {
							if (i == 1) {
								String str = record.get(i).toString()
										.split(",")[0];
								hcat.set(i, str);
							} else {
								hcat.set(i, record.get(i));
							}
						}
						return new SerializableWritable<HCatRecord>(hcat);
					}
				});
		return result;
	}

	@SuppressWarnings("rawtypes")
	public static void store(JavaRDD<SerializableWritable<HCatRecord>> rdd,
			String dbName, String tblName) {
		Job outputJob = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("lowerUpperCaseConvert");
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
