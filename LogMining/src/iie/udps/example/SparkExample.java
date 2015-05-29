package iie.udps.example;

import iie.udps.common.hcatalog.SerHCatInputFormat;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.thrift.TException;

import scala.Tuple2;

/**
 * 实现功能：spark+hcatlog 读hive表数据，将其中一列数据大写后写会另一张hive表中
 * 
 * @author Administrator
 *
 */
public class SparkExample {

	public static void main(String[] args) throws IOException {
		String dbName = "xdf";
		String inputTable = "input_tbl";
		String outputTable = "output_tbl";
		int fieldPosition = 1;//要转换成大写的字段
		
        //创建JavaSparkContext作为spark程序入口
		SparkConf sparkConf = new SparkConf().setAppName("SparkExample");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// 读取hive表message字段数据，进行分词，去干扰词，过滤处理，返回RDD类型数据
		JavaRDD<SerializableWritable<HCatRecord>> LastRDD = getProcessedData(
				jsc, dbName, inputTable, fieldPosition);
		
//		// 调用hive sql生成中间输出表表processingOutTable保存数据
//		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(
//				jsc.sc());
//		sqlContext.sql("create table " + dbName + "." + outputTable + "("
//				+ outputTableSchema + ")");

		// 根据输入表结构，创建同样结构的输出表
		HCatSchema schema = getHCatSchema(dbName, inputTable);
		createTable(dbName, outputTable, schema);
		
		// 将处理后的数据存到输出表中
		storeToTable(LastRDD, dbName, outputTable);
		jsc.stop();
		System.exit(0);

	}

	@SuppressWarnings("rawtypes")
	public static JavaRDD<SerializableWritable<HCatRecord>> getProcessedData(
			JavaSparkContext jsc, String dbName, String inputTable,
			int fieldPosition) throws IOException {
		// 处理数据
		Configuration inputConf = new Configuration();
		Job job = Job.getInstance(inputConf);
		SerHCatInputFormat.setInput(job.getConfiguration(), dbName, inputTable);
		JavaPairRDD<WritableComparable, SerializableWritable> rdd = jsc
				.newAPIHadoopRDD(job.getConfiguration(),
						SerHCatInputFormat.class, WritableComparable.class,
						SerializableWritable.class);

		final Broadcast<Integer> posBc = jsc.broadcast(fieldPosition);// 将数据广播出去
		// 获取表记录集
		JavaRDD<SerializableWritable<HCatRecord>> messageRDD = rdd
				.map(new Function<Tuple2<WritableComparable, SerializableWritable>, SerializableWritable<HCatRecord>>() {
					private static final long serialVersionUID = 1L;
					private final int postion = posBc.getValue().intValue();

					@Override
					public SerializableWritable<HCatRecord> call(
							Tuple2<WritableComparable, SerializableWritable> arg0)
							throws Exception {
						HCatRecord record = (HCatRecord) arg0._2.value();
						HCatRecord hcat = new DefaultHCatRecord(record.size());
						for (int i = 0; i < record.size(); i++) {
							if (postion == i) {
								hcat.set(i, record.get(i).toString()
										.toUpperCase());
							} else {
								hcat.set(i, record.get(i));
							}
						}
						return new SerializableWritable<HCatRecord>(hcat);
					}
				});
		// 返回处理后的数据
		return messageRDD;
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
			outputJob.setJobName("SparkExample");
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
	 * 获得表模式
	 * 
	 * @param dbName
	 * @param tblName
	 * @return
	 */
	public static HCatSchema getHCatSchema(String dbName, String tblName) {
		Job outputJob = null;
		HCatSchema schema = null;
		try {
			outputJob = Job.getInstance();
			outputJob.setJobName("getHCatSchema");
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);
			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tblName, null));
			schema = SerHCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return schema;
	}

	/**
	 * 指定数据库、表名，创建表
	 * 
	 * @param dbName
	 * @param tblName
	 * @param schema
	 */
	public static void createTable(String dbName, String tblName,
			HCatSchema schema) {
		HiveMetaStoreClient client = null;
		try {
			HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
			client = HCatUtil.getHiveClient(hiveConf);
		} catch (MetaException | IOException e) {
			e.printStackTrace();
		}
		try {
			if (client.tableExists(dbName, tblName)) {
				client.dropTable(dbName, tblName);
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		// 获取表模式
		List<FieldSchema> fields = HCatUtil.getFieldSchemaList(schema
				.getFields());
		// 生成表对象
		Table table = new Table();
		table.setDbName(dbName);
		table.setTableName(tblName);

		StorageDescriptor sd = new StorageDescriptor();
		sd.setCols(fields);
		table.setSd(sd);
		sd.setInputFormat(RCFileInputFormat.class.getName());
		sd.setOutputFormat(RCFileOutputFormat.class.getName());
		sd.setParameters(new HashMap<String, String>());
		sd.setSerdeInfo(new SerDeInfo());
		sd.getSerdeInfo().setName(table.getTableName());
		sd.getSerdeInfo().setParameters(new HashMap<String, String>());
		sd.getSerdeInfo().getParameters()
				.put(serdeConstants.SERIALIZATION_FORMAT, "1");
		sd.getSerdeInfo().setSerializationLib(
				org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class
						.getName());
		Map<String, String> tableParams = new HashMap<String, String>();
		table.setParameters(tableParams);
		try {
			client.createTable(table);
			System.out.println("Create table successfully!");
		} catch (TException e) {
			e.printStackTrace();
			return;
		} finally {
			client.close();
		}
	}
}
