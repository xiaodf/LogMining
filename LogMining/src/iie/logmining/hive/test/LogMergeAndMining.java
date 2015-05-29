package iie.logmining.hive.test;

import iie.logmining.hive.train.OperatorParamXml;

import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
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
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.apache.spark.sql.hive.HiveContext;

/*
 * 功能：合并加上类别后的日志和告警数据，分割生成事务数据，根据事务数据建立fptree存到hdfs文件中
 * 命令：
 * spark-submit --class iie.logmining.data.train.LogMergeAndMining 
 * --master yarn-client --num-executors 40 --driver-memory 512m  --executor-memory 1g  --executor-cores 1
 * --jars /home/xdf/udps-sdk-0.0.1.jar,/home/xdf/jars/lucene-analyzers-common-4.10.2.jar,/home/xdf/spark-mllib_2.10-1.3.0.jar
 *  /home/xdf/test5.jar -c /user/xdf/stdin.xml
 */

public class LogMergeAndMining {

	public static SimpleDateFormat data_template = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	public static Stack<TreeNode> stack = new Stack<TreeNode>();
	public static HashSet<String> alarm_label_set = new HashSet<String>();
	public static String root_leaf_path = "";

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: <-c> <stdin.xml>");
			System.exit(1);
		}
		String stdinXml = args[1];
		OperatorParamXml operXML = new OperatorParamXml();
		List<Map> list = operXML.parseStdinXml(stdinXml);// 参数列表

		String dbName = list.get(0).get("dbName").toString(); // 数据库
		// String labelOutTable = list.get(0).get("labelOutTable").toString();
		// // 带类别的数据输入表
		// String alarmTable = list.get(0).get("alarmTable").toString(); //
		// 告警日志数据输入表
		String labelOutTable = "labelmatch1"; // 带类别的数据输入表
		String alarmTable = "alarm_partition"; // 告警日志数据输入表
		// String fptreePath = list.get(0).get("fptreePath").toString(); //
		// 输出fptree结果的hdfs目录
		String fptreePath = "/user/xdf/fptreePath7/";
		double minSupport = Double.parseDouble(list.get(0).get("minSupport")
				.toString()); // 最小支持度
//		int windowSize = Integer.parseInt(list.get(0).get("windowSize")
//				.toString());// 窗口大小，分钟为单位

		// String partition =
		// "timepartition >='2014-07-31' and timepartition <='2015-03-26'";
		int windowSize = 10;
		int stepSize = 5;
		String partition = "timepartition ='2014-08-14'";
		//int stepSize = Integer.parseInt(list.get(0).get("stepSize").toString());// 步长大小，分钟为单位
		List<String> ips = (List<String>) list.get(1).get("ips");
		SparkConf sparkConf = new SparkConf().setAppName("LogMergeAndMining");
		SparkContext sc = new SparkContext(sparkConf);
		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		for (int k = 0; k < ips.size(); k++) {
			// 获取对应ip的日志数据和告警数据
			String ip = ips.get(k);
			List<List<String>> logMergeList = getMergeLogList(sqlContext,
					dbName, labelOutTable, alarmTable, ip, partition);
			if (logMergeList.size() == 0) {
				break;
			}

			// 每隔10分钟，取30分钟内的出现的日志类别作为一个事务(类1,类2...)
			List<List<String>> transactions = getTransactions(logMergeList,
					windowSize, stepSize);
			if (transactions.size() == 0) {
				break;
			}
			System.out.println("transactions====" + transactions.size());
			for (int q = 0; q < transactions.size(); q++) {
				System.out.println("----------------------------");
				
				System.out.println("transactions====" + transactions.get(q));
			}
			// 创建FPTree，进行频繁项集挖掘
			// buildFPTree(transactions, fptreePath, minSupport, ip);
			// alarm_label_set.clear();
		}
		sc.stop();
		System.exit(0);
	}

	/**
	 * 从类别表和告警数据表读取时间和类别字段数据，按时间排序合并
	 */
	public static List<List<String>> getMergeLogList(HiveContext sqlContext,
			String dbName, String labelOutTable, String alarmTable, String ip,
			String partition) {

		Row[] rows = sqlContext.sql(
				"select * from (select distinct time,label from " + dbName
						+ "." + labelOutTable + " where ip ='" + ip + "'and "
						+ partition
						+ " union all select distinct time,label from "
						+ dbName + "." + alarmTable + " where ip ='" + ip
						+ "' and " + partition + ")as t order by time")
				.collect();
		Row[] alarms = sqlContext.sql(
				"select distinct label from " + dbName + "." + alarmTable
						+ " where ip ='" + ip + "' and " + partition ).collect();
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
			tempList.add(rows[i].getString(0));// time
			tempList.add(rows[i].getString(1));// label
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
		int curIndex = 0;// 当前日志的索引
		int index = 0;// 当前日志的索引
		while (minDate.getTime() < endDate.getTime()) {
			// 设置步长时刻时间,用来保存步长时刻日志序号
			cal.setTime(minDate);
			cal.add(Calendar.MINUTE, stepSize);
			Date stepSizeDate = cal.getTime();
			// 保存当前时间窗口内的类别数据到set集合中
			HashSet<String> curLogSet = new HashSet<String>();
			curIndex = index;
			System.out.println("curIndex======="+curIndex);
			System.out.println("stepSizeDate======="+stepSizeDate);
			System.out.println("minDate======="+minDate);
			System.out.println("maxDate======="+maxDate);
			for (int i = curIndex; i < logMergeListLength; i++) {
				Date timeStamp = data_template
						.parse(logMergeList.get(i).get(0));
				if (timeStamp.getTime() >= minDate.getTime()
						&& timeStamp.getTime() < maxDate.getTime()) {
					curLogSet.add(logMergeList.get(i).get(1));
				} else if (timeStamp.getTime() >= maxDate.getTime()) {
					break;
				}
				if (timeStamp.getTime() <= stepSizeDate.getTime()) {
					index++;
				}
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

	/**
	 * 创建FPTree，进行频繁项集挖掘
	 */
	public static void buildFPTree(List<List<String>> transactions,
			String fptreePath, double minSupport, String ip) {
		LogMergeAndMining fptree = new LogMergeAndMining();
		ArrayList<TreeNode> F1 = fptree.buildF1Items(transactions, minSupport);
		TreeNode treeroot = fptree.buildFPTree(transactions, F1);
		// 打印FPTree树
		try {
			Configuration conf = new Configuration();
			String filePath = fptreePath + "fptree" + ip;
			FileSystem fs = FileSystem.get(conf);
			OutputStream out = fs.create(new Path(filePath));
			printTree(treeroot, "", true, out);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// 打印FPTree根节点到叶节点路径
		try {
			Configuration conf2 = new Configuration();
			String filePath2 = fptreePath + "fptreeBranch" + ip;
			FileSystem fs2 = FileSystem.get(conf2);
			OutputStream out2 = fs2.create(new Path(filePath2));
			printTreePaths(treeroot, out2);// 打印FPTree根节点到叶节点路径
			out2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 构造频繁1项集
	 */
	public ArrayList<TreeNode> buildF1Items(List<List<String>> transRecords,
			double minSupport) {
		ArrayList<TreeNode> F1 = null;
		if (transRecords.size() > 0) {
			F1 = new ArrayList<TreeNode>();
			Map<String, TreeNode> map = new HashMap<String, TreeNode>();
			// 计算事务数据库中各项的支持度
			for (List<String> record : transRecords) {
				for (String item : record) {
					if (!map.keySet().contains(item)) {
						TreeNode node = new TreeNode(item);
						node.setCount(1);
						map.put(item, node);
					} else {
						map.get(item).countIncrement(1);
					}
				}
			}
			// 把支持度大于（或等于）minSup的项加入到F1中
			Set<String> names = map.keySet();
			for (String name : names) {
				TreeNode tnode = map.get(name);
				if (tnode.getCount() >= minSupport) {
					F1.add(tnode);
				}
			}
			Collections.sort(F1);
			return F1;
		} else {
			return null;
		}
	}

	/**
	 * 建立FP-Tree
	 */
	public TreeNode buildFPTree(List<List<String>> transRecords,
			ArrayList<TreeNode> F1) {
		TreeNode root = new TreeNode("root"); // 创建树的根节点
		for (List<String> transRecord : transRecords) {
			LinkedList<String> record = sortByF1(transRecord, F1);
			TreeNode subTreeRoot = root;
			TreeNode tmpRoot = null;
			if (root.getChildren() != null) {
				while (!record.isEmpty()
						&& (tmpRoot = subTreeRoot.findChild(record.peek())) != null) {// peek获取首元素
					tmpRoot.countIncrement(1);
					subTreeRoot = tmpRoot;// 层次遍历
					record.poll();// poll删除首元素
				}
			}
			addNodes(subTreeRoot, record, F1);
		}
		return root;
	}

	/**
	 * 把事务数据库中的一条记录按照F1（频繁1项集）中的顺序排序
	 */
	public LinkedList<String> sortByF1(List<String> transRecord,
			ArrayList<TreeNode> F1) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (String item : transRecord) {
			// 由于F1已经是按降序排列的，
			for (int i = 0; i < F1.size(); i++) {
				TreeNode tnode = F1.get(i);
				if (tnode.getName().equals(item)) {
					map.put(item, i);
				}
			}
		}
		ArrayList<Entry<String, Integer>> al = new ArrayList<Entry<String, Integer>>(
				map.entrySet());
		Collections.sort(al, new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> arg0,
					Entry<String, Integer> arg1) {
				// 降序排列
				return arg0.getValue() - arg1.getValue();
			}
		});
		LinkedList<String> rest = new LinkedList<String>();
		for (Entry<String, Integer> entry : al) {
			rest.add(entry.getKey());
		}
		return rest;
	}

	/**
	 * 把若干个节点作为指定节点的后代插入树中
	 */
	public void addNodes(TreeNode ancestor, LinkedList<String> record,
			ArrayList<TreeNode> F1) {
		if (record.size() > 0) {
			while (record.size() > 0) {
				String item = record.poll();
				TreeNode leafnode = new TreeNode(item);
				leafnode.setCount(1);
				leafnode.setParent(ancestor);
				ancestor.addChild(leafnode);

				for (TreeNode f1 : F1) {
					if (f1.getName().equals(item)) {
						while (f1.getNextHomonym() != null) {
							f1 = f1.getNextHomonym();
						}
						f1.setNextHomonym(leafnode);
						break;
					}
				}
				addNodes(leafnode, record, F1);
			}
		}
	}

	/**
	 * 打印多叉树根节点到叶子节点路径(类似后序遍历) 树根节点
	 */
	public static void printTreePaths(TreeNode node, OutputStream out) {
		if (node != null) {
			stack.add(node);
			if (node.getChildren() == null || node.getChildren().size() == 0) {
				for (TreeNode n : stack) {
					root_leaf_path += n.getName() + ":" + n.getCount() + " ";
				}
				try {
					out.write(root_leaf_path.getBytes());
					out.write("\n".getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}
				root_leaf_path = "";// 初始化路径变量
				stack.pop();
				return;
			}
			for (int i = 0; i < node.getChildren().size(); i++) {
				printTreePaths(node.getChildren().get(i), out);
			}
			stack.pop();
		}
	}

	/**
	 * 打印多叉树，并把打印出的树写入文件。
	 * */
	public static void printTree(TreeNode treeNode, String prefix,
			boolean isTail, OutputStream out) {
		try {
			String str = prefix + (isTail ? "└── " : "├── ")
					+ treeNode.getName() + " :" + treeNode.getCount() + "\r\n";
			out.write(str.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (treeNode.getChildren() == null)
			return;
		for (int i = 0; i < treeNode.getChildren().size() - 1; i++) {
			printTree(treeNode.getChildren().get(i), prefix
					+ (isTail ? "    " : "│   "), false, out);
		}
		if (treeNode.getChildren().size() > 0) {
			printTree(
					treeNode.getChildren().get(
							treeNode.getChildren().size() - 1), prefix
							+ (isTail ? "    " : "│   "), true, out);
		}
	}
}
