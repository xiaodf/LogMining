package iie.logmining.hive.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.apache.spark.sql.hive.HiveContext;

public class PartitionTest {
	public static SimpleDateFormat data_template = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	public static Stack<TreeNode> stack = new Stack<TreeNode>();
	public static HashSet<String> alarm_label_set = new HashSet<String>();
	public static String root_leaf_path = "";

	public static void main(String[] args) throws ParseException {
		String dbName = "xdf"; // 数据库
		String labelOutTable = "logs5_partition"; // 带类别的数据输入表
		String alarmTable = "alarm_partition"; // 告警日志数据输入表
		String fptreePath = "/user/xdf/fptreePath"; // 输出fptree结果的hdfs目录
		double minSupport = 0.5; // 最小支持度
		int windowSize = 30;// 窗口大小，分钟为单位
		int stepSize = 30;// 步长大小，分钟为单位
		String partition = "timepartition <='2014-08-14' and timepartition >='2014-07-31'";
		String ip ="192.168.8.3";

		SparkConf sparkConf = new SparkConf().setAppName("LogMergeAndMining");
		SparkContext sc = new SparkContext(sparkConf);
		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		List<List<String>> logMergeList = getMergeLogList(sqlContext,
				dbName, labelOutTable, alarmTable, ip,partition);

		// 每隔10分钟，取30分钟内的出现的日志类别作为一个事务(类1,类2...)
		List<List<String>> transactions = getTransactions(logMergeList,windowSize, stepSize);
	 
		for(int i = 0;i<transactions.size();i++){
			System.out.println(transactions.get(i));
		}
	    sc.stop();
	    System.exit(0);
	}

	/**
	 * 从类别表和告警数据表读取时间和类别字段数据，按时间排序合并
	 */
	public static List<List<String>> getMergeLogList(HiveContext sqlContext,
			String dbName, String labelOutTable, String alarmTable, String ip,String partition) {

		Row[] rows = sqlContext.sql(
				"select * from (select time,label from " + dbName + "."
						+ labelOutTable + " where ip ='" + ip
						+ "' and "+ partition+ "union all select time,label from " + dbName + "."
						+ alarmTable + " where ip ='" + ip
						+ "' and "+ partition+ ")as t order by time").collect();
		Row[] alarms = sqlContext.sql(
				"select distinct label from " + dbName + "." + alarmTable
						+ " where ip ='" + ip + "' and "+ partition).collect();
		if (rows.length == 0 || alarms.length == 0) {
			return null;
		}
		// 获得告警数据类别放到set集合中
		for (int i = 0; i < alarms.length; i++) {
			alarm_label_set.add(alarms[i].getString(0));
		}
		// 将log和告警数据按ip查出按时间排序后放到数组中
		List<List<String>> logMergeList = new ArrayList<List<String>>();// 日志文件和告警文件合并后的数据集合
		for (int i = 0; i < rows.length; i++) {
			List<String> tempList = new ArrayList<String>();
			if ("".equals(rows[i].getString(0))
					|| "".equals(rows[i].getString(1))) {
				continue;
			}
			tempList.add(rows[i].getString(0));
			tempList.add(rows[i].getString(1));
			logMergeList.add(tempList);
		}
		return logMergeList;
	}

	/**
	 * 每隔10分钟，取30分钟内的出现的日志类别作为一个事务(类1,类2...)
	 */
	public static List<List<String>> getTransactions(
			List<List<String>> logMergeList, int windowSize, int stepSize)
			throws ParseException {

		List<List<String>> transactions = new ArrayList<List<String>>();// 用来存放事务
		int logMergeListLength = logMergeList.size();// 数据集大小
		// 获取合并后的数据集的最早时间
		Date minDate = data_template.parse(logMergeList.get(0).get(0));
		// 获取合并后的数据集的最晚时间
		Date endDate = data_template.parse(logMergeList.get(
				logMergeListLength - 1).get(0));
		// 设置开始时间
		Calendar cal = Calendar.getInstance();
		cal.setTime(minDate);
		// 设置时间窗口大小
		cal.add(Calendar.MINUTE, windowSize);
		Date maxDate = cal.getTime();
		int curIndex = 0;// 当前时间
		while (minDate.getTime() < endDate.getTime()) {
			// 保存当前时间窗口内的类别数据到set集合中
			HashSet<String> curLogSet = new HashSet<String>();
			for (int i = curIndex; i < logMergeListLength; i++) {
				Date timeStamp = data_template
						.parse(logMergeList.get(i).get(0));
				if (timeStamp.getTime() >= minDate.getTime()
						&& timeStamp.getTime() < maxDate.getTime()) {
					curLogSet.add(logMergeList.get(i).get(1));
				} else if (timeStamp.getTime() >= maxDate.getTime()) {
					break;
				}
				curIndex++;
			}
			// 判断当前时间窗口内是否含有告警类别,类别总数是否大于一个，若有则将数据加入事务集，否则忽略
			if (curLogSet.size() > 1) {
				// 将set集合中的数据放到List集合中
				List<String> curLogList = new ArrayList<String>();
				Iterator<String> it = curLogSet.iterator();
				while (it.hasNext()) {
					curLogList.add(it.next());
				}
				boolean ifExistWarn = false;
				for (int j = 0; j < curLogSet.size(); j++) {
					if (ifExistWarn == true) {
						break;
					}
					// 如果该事务项集存在告警日志，则存入事务数据集
					if (alarm_label_set.contains(curLogList.get(j))) {
						ifExistWarn = true;
					}
				}
				if (ifExistWarn) {
					// 添加一个事务
					transactions.add(curLogList);
				}
			}
			// 窗口最大最小时间下滑一个步长
			cal.setTime(minDate);
			cal.add(Calendar.MINUTE, stepSize);
			minDate = cal.getTime();
			cal.setTime(maxDate);
			cal.add(Calendar.MINUTE, stepSize);
			maxDate = cal.getTime();
		}
		return transactions;
	}

}
