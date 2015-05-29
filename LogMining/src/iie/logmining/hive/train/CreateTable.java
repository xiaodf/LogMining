package iie.logmining.hive.train;

import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/*
 * spark-submit --class iie.logmining.hive.train.CreateTable --master yarn-cluster 
 * --jars /home/xdf/udps-sdk-0.0.1.jar,/home/xdf/spark-mllib_2.10-1.3.0.jar
 *  /home/xdf/run/test.jar -c /user/xdf/stdin.xml
 */
public class CreateTable {

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
		String processingTabSchema = list.get(0).get("processingTabSchema")
				.toString(); // 输出表结构
		String addIDOutTable = list.get(0).get("addIDOutTable").toString(); // 输出表
		String addIDTabSchema = list.get(0).get("addIDTabSchema").toString(); // 输出表结构
		String featureTable = list.get(0).get("featureTable").toString(); // 输出表
		String featureTabSchema = list.get(0).get("featureTabSchema")
				.toString(); // 输出表结构
		String labelOutTable = list.get(0).get("labelOutTable").toString(); // 输出表
		String labelTabSchema = list.get(0).get("labelTabSchema").toString(); // 输出表结构

		// 创建输出表
		SparkConf sparkConf = new SparkConf().setAppName("CreateTable");
		SparkContext sc = new SparkContext(sparkConf);
		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
		System.out.println("create table " + dbName + "." + processingOutTable);
		sqlContext.sql("create table " + dbName + "." + processingOutTable
				+ "(" + processingTabSchema + ")");

		System.out.println("create table " + dbName + "." + processingOutTable);
		sqlContext.sql("create table " + dbName + "." + addIDOutTable + "("
				+ addIDTabSchema + ")");

		System.out.println("create table " + dbName + "." + featureTable);
		sqlContext.sql("create table " + dbName + "." + featureTable + "("
				+ featureTabSchema + ")");

		System.out.println("create table " + dbName + "." + labelOutTable);
		sqlContext.sql("create table " + dbName + "." + labelOutTable + "("
				+ labelTabSchema + ")");
		sc.stop();
		System.exit(0);
	}
}
