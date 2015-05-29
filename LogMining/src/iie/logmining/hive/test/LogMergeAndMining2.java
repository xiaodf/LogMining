package iie.logmining.hive.test;

import iie.logmining.example.Item;
import iie.logmining.hive.train.OperatorParamXml;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.apache.spark.sql.hive.HiveContext;

import scala.Function;
import scala.Function1;
import scala.runtime.BoxedUnit;

import com.google.common.base.Joiner;

/*
 * 功能：合并加上类别后的日志和告警数据，分割生成事务数据，根据事务数据建立fptree存到hdfs文件中
 * 命令：
 * spark-submit --class iie.logmining.data.train.LogMergeAndMining --master yarn-client 
 * --jars /home/xdf/udps-sdk-0.0.1.jar,/home/xdf/jars/lucene-analyzers-common-4.10.2.jar,/home/xdf/spark-mllib_2.10-1.3.0.jar
 *  /home/xdf/test5.jar -c /user/xdf/stdin.xml
 */

public class LogMergeAndMining2 {
	public static SimpleDateFormat data_template = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	public static Stack<TreeNode> stack = new Stack<TreeNode>();
	public static HashSet<String> alarm_label_set = new HashSet<String>();
	public static String root_leaf_path = "";
	public static double minsup = 0.6;
	public static double minconf = 0.5;

	@SuppressWarnings({ "rawtypes" })
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}
		String stdinXml = args[1];
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(stdinXml);// 参数列表

		String dbName = list.get(0).get("dbName").toString(); // 数据库
		String labelOutTable = list.get(0).get("labelOutTable").toString(); // 带类别的数据输入表
		String alarmTable = list.get(0).get("alarmTable").toString(); // 告警日志数据输入表
		String fptreePath = list.get(0).get("fptreePath").toString(); // 输出fptree结果的hdfs目录
		minconf = 0.5;
		minsup = 0.6;
		double minSupport = Double.parseDouble(list.get(0).get("minSupport")
				.toString()); // 最小支持度
		int windowSize = Integer.parseInt(list.get(0).get("windowSize")
				.toString());// 窗口大小，分钟为单位
		int stepSize = Integer.parseInt(list.get(0).get("stepSize").toString());// 步长大小，分钟为单位
		// List<String> ips = (List<String>) list.get(1).get("ips");
		List<String> ips = new ArrayList<String>();
		ips.add("192.168.8.1");

		SparkConf sparkConf = new SparkConf().setAppName("LogMergeAndMining");
		sparkConf.set("spark.driver.maxResultSize", "3g");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(
				jsc.sc());

		for (int k = 0; k < ips.size(); k++) {
			// 获取对应ip的日志数据和告警数据
			String ip = ips.get(k);
			Row[] rows = sqlContext.sql(
					"select * from (select time,label from " + dbName + "."
							+ labelOutTable + " where ip ='" + ip
							+ "' union all select time,label from " + dbName
							+ "." + alarmTable + " where ip ='" + ip
							+ "')as t order by time").collect();
			JavaSchemaRDD temp = sqlContext.sql(
					"select * from (select time,label from " + dbName + "."
							+ labelOutTable + " where ip ='" + ip
							+ "' union all select time,label from " + dbName
							+ "." + alarmTable + " where ip ='" + ip
							+ "')as t order by time").toJavaSchemaRDD();
		
			Row[] alarms = sqlContext.sql(
					"select distinct label from " + dbName + "." + alarmTable
							+ " where ip ='" + ip + "'").collect();
			if (rows.length == 0 || alarms.length == 0)
				break;
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
			// 每隔10分钟，取30分钟内的出现的日志类别作为一个事务(类1,类2...)
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
					Date timeStamp = data_template.parse(logMergeList.get(i)
							.get(0));
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
			// 创建FPTree，进行频繁项集挖掘
			System.out.println("Begin Build FPTree...");
			Long count = (long) transactions.size();
			JavaRDD<List<String>> transactionsRDD = jsc.parallelize(
					transactions).cache();
			// FPGrowth频繁项集挖掘
			FPGrowth fpg = new FPGrowth().setMinSupport(minsup);
			FPGrowthModel<String> model = fpg.run(transactionsRDD);
			System.out.println("1111111111111111111111...");
			System.out.println(count);
			System.out.println(model.freqItemsets().count());
			System.out.println(model.freqItemsets().first().javaItems()+": "+model.freqItemsets().first().freq());
			System.out.println();
			
			/*
			 * 获得频繁项集和频率, 记录频繁项集的时候，首先用set记录一下每个频繁项中的元素并按照set默认的排序规则进行排序，
			 * 这样频繁项集中的元素就都是按照同一种排序规则进行排列的了
			 */
			//Set<String> freqitem = new HashSet<String>();
			//Map<String, Long> freq = new HashMap<String, Long>();
//			for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets()
//					.toJavaRDD().collect()) {
//				System.out.println("["
//						+ Joiner.on(",").join(itemset.javaItems()) + "], "
//						+ itemset.freq());
////				for (String s : itemset.javaItems()) {// 用set把元素按照默认的规则进行排序处理
////					freqitem.add(s);
////				}
//				// 用freq记录频繁项集
////				freq.put(Joiner.on(",").join(freqitem), itemset.freq());
////				freqitem.clear();
//			}

			// System.out.println("check if it's sorted");
			// for (String key : freq.keySet()) {
			// System.out.println(key);
			// }

			// 获得关联规则
			List<Item> rules = new ArrayList<Item>();
			// 对每一个k频繁项，分别求1->k-1,2->k-2.....k-1->1的关联规则
//			for (String key : freq.keySet()) {
//				rules.addAll(subGen(key, freq, count));
//			}
			// 输出关联规则
			System.out.println("rules:-------------" + rules.size());
			// for (Item item : rules) {
			// System.out.println(item.getLfs() + "->" + item.getRfs() + "\t"
			// + item.getSupport() + "\t" + item.getConfidence()
			// + "\t" + item.getLift());
			// }
		}
		jsc.stop();
		System.exit(0);
	}

	public static List<Item> subGen(String ss, Map<String, Long> freq,
			Long count) {
		String[] s = ss.split(",");
		Long freqcurrent = freq.get(ss);
		Set<String> x = new HashSet<String>();
		Set<String> y = new HashSet<String>();
		String lfs = "";
		String rfs = "";
		List<Item> result = new ArrayList<Item>();

		// i表示s.length长的二进制数，所有取1的可能组合
		// 里面的循环用来分别把取1位置上的元素读到lfs，取0位置上的元素读到rfs
		for (int i = 1; i < (1 << s.length) - 1; i++) {
			for (int j = 0; j < s.length; j++) {
				if (((1 << j) & i) != 0) {
					x.add(s[j]);
					if (lfs.equals(""))
						lfs += s[j];
					else
						lfs += "," + s[j];
				} else {
					y.add(s[j]);
					if (rfs.equals(""))
						rfs += s[j];
					else
						rfs += "," + s[j];
				}
			}
			// System.out.println("test");
			// System.out.println("x.size()=" + x.size());
			// System.out.println(freqcurrent);
			// System.out.println(freq.get(lfs));
			double confidence = (double) freqcurrent / freq.get(lfs);
			// System.out.println(confidence);
			if (confidence >= minconf) {
				double support = (double) freqcurrent / count;
				double lift = (double) confidence / freq.get(rfs) * count;
				// System.out.println("inside:" + lfs + "->" + rfs + "\t"
				// + support + "\t" + confidence + "\t" + lift);
				Item item = new Item(lfs, rfs, support, confidence, lift);
				result.add(item);
			}
			x.clear();
			y.clear();
			lfs = "";
			rfs = "";
		}
		return result;
	}
}
