package iie.logmining.hive.test;

import iie.logmining.hive.train.OperatorParamXml;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/*
 * spark-submit --class iie.logmining.hive.test.DropTable --master yarn-cluster 
 * --jars /home/xdf/udps-sdk-0.0.1.jar,/home/xdf/jars/lucene-analyzers-common-4.10.2.jar,/home/xdf/spark-mllib_2.10-1.3.0.jar
 * /home/xdf/run/test6.jar -c /user/xdf/stdin.xml
 */
public class DropTable {

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
		String processingOutTable = list.get(0).get("processingOutTable")
				.toString(); // 输出表
		String addIDOutTable = list.get(0).get("addIDOutTable").toString(); // 输出表
		String labelOutTable = list.get(0).get("labelOutTable").toString(); // 输出表

		// 创建输出表
		SparkConf sparkConf = new SparkConf().setAppName("CreateTable");
		SparkContext sc = new SparkContext(sparkConf);
		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
		System.out.println("drop table " + dbName + "." + processingOutTable);
		sqlContext.sql("drop table " + dbName + "." + processingOutTable);
		System.out.println("drop table " + dbName + "." + addIDOutTable);
		sqlContext.sql("drop table " + dbName + "." + addIDOutTable);
		System.out.println("create table " + dbName + "." + labelOutTable);
		sqlContext.sql("drop table " + dbName + "." + labelOutTable);

		sc.stop();
		System.exit(0);
	}
}
