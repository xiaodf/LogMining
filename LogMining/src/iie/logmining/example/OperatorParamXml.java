package iie.logmining.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 * Dom4j 生成XML文档与解析XML文档
 */
public class OperatorParamXml {

	@SuppressWarnings("rawtypes")
	public List<Map> parseStdinXml(String stdinXml) throws Exception {

		// 读取stdin.xml文件
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dis = fs.open(new Path(stdinXml));
		InputStreamReader isr = new InputStreamReader(dis, "utf-8");
		BufferedReader read = new BufferedReader(isr);
		String tempString = "";
		String xmlParams = "";
		while ((tempString = read.readLine()) != null) {
			xmlParams += "\n" + tempString;
		}
		read.close();
		xmlParams = xmlParams.substring(1);

		String jobinstanceid = null;
		String dbName = null;
		String sourceTable = null;
		String partition = null;
		String timeFieldPos = null;
		String ipFieldPos = null;
		String messageFieldPos = null;
		String processingOutTable = null;
		String processingTabSchema = null;
		String addIDOutTable = null;
		String addIDTabSchema = null;
		String featureTable = null;
		String featureTabSchema = null;
		String similarity = null;
		String labelOutTable = null;
		String labelTabSchema = null;
		String inputFilePath = null;
		String fptreePath = null;
		String windowSize = null;
		String stepSize = null;
		String minSupport = null;
		String alarmTable = null;
		String alarmTimeField = null;
		String alarmLabelField = null;
		List<String> ips = new ArrayList<String>();

		List<Map> list = new ArrayList<Map>();
		Map<String, String> map = new HashMap<String, String>();
		Map<String, List<String>> ipMap = new HashMap<String, List<String>>();
		Document document = DocumentHelper.parseText(xmlParams); // 将字符串转化为xml
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator iter1 = node1.elementIterator(); // 获取根节点下的子节点
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			// 获取jobinstanceid
			if ("jobinstanceid".equals(node2.getName())) {
				jobinstanceid = node2.getText();
				map.put("jobinstanceid", jobinstanceid);
				System.out.println("====jobinstanceid=====" + jobinstanceid);
			}
			if ("operator".equals(node2.getName())) {
				if ("DataCleaning".equals(node2.attributeValue("name"))) {
					Iterator iter2 = node2.elementIterator();
					while (iter2.hasNext()) {
						Element node3 = (Element) iter2.next();
						if ("parameter".equals(node3.getName())) {
							if ("inputFilePath".equals(node3
									.attributeValue("name"))) {
								inputFilePath = node3.getText();
								map.put("inputFilePath", inputFilePath);
							}
						}
					}
				}
				if ("HiveDataProcessing".equals(node2.attributeValue("name"))) {
					Iterator iter2 = node2.elementIterator();
					while (iter2.hasNext()) {
						Element node3 = (Element) iter2.next();
						if ("parameter".equals(node3.getName())) {
							if ("processingOutTable".equals(node3
									.attributeValue("name"))) {
								processingOutTable = node3.getText();
								map.put("processingOutTable",
										processingOutTable);
							}
							if ("processingTabSchema".equals(node3
									.attributeValue("name"))) {
								processingTabSchema = node3.getText();
								map.put("processingTabSchema",
										processingTabSchema);
							}
						}
					}

				}
				if ("AddIDs".equals(node2.attributeValue("name"))) {
					Iterator iter2 = node2.elementIterator();
					while (iter2.hasNext()) {
						Element node3 = (Element) iter2.next();
						if ("parameter".equals(node3.getName())) {
							if ("addIDOutTable".equals(node3
									.attributeValue("name"))) {
								addIDOutTable = node3.getText();
								map.put("addIDOutTable", addIDOutTable);
							}
							if ("addIDTabSchema".equals(node3
									.attributeValue("name"))) {
								addIDTabSchema = node3.getText();
								map.put("addIDTabSchema", addIDTabSchema);
							}
						}
					}
				}
				if ("DirectGenerateFeature"
						.equals(node2.attributeValue("name"))) {
					Iterator iter2 = node2.elementIterator();
					while (iter2.hasNext()) {
						Element node3 = (Element) iter2.next();
						if ("parameter".equals(node3.getName())) {
							if ("featureTable".equals(node3
									.attributeValue("name"))) {
								featureTable = node3.getText();
								map.put("featureTable", featureTable);
							}
							if ("featureTabSchema".equals(node3
									.attributeValue("name"))) {
								featureTabSchema = node3.getText();
								map.put("featureTabSchema", featureTabSchema);
							}
							if ("similarity".equals(node3
									.attributeValue("name"))) {
								similarity = node3.getText();
								map.put("similarity", similarity);
							}
						}
					}
				}
				if ("AddLabel".equals(node2
						.attributeValue("name"))) {
					Iterator iter2 = node2.elementIterator();
					while (iter2.hasNext()) {
						Element node3 = (Element) iter2.next();
						if ("parameter".equals(node3.getName())) {
							if ("labelOutTable".equals(node3
									.attributeValue("name"))) {
								labelOutTable = node3.getText();
								map.put("labelOutTable", labelOutTable);
							}
							if ("labelTabSchema".equals(node3
									.attributeValue("name"))) {
								labelTabSchema = node3.getText();
								map.put("labelTabSchema", labelTabSchema);
							}
						}
					}
				}
				if ("LogMergeAndMining".equals(node2.attributeValue("name"))) {
					Iterator iter2 = node2.elementIterator();
					while (iter2.hasNext()) {
						Element node3 = (Element) iter2.next();
						if ("parameter".equals(node3.getName())) {
							if ("fptreePath".equals(node3
									.attributeValue("name"))) {
								fptreePath = node3.getText();
								map.put("fptreePath", fptreePath);
							}
							if ("windowSize".equals(node3
									.attributeValue("name"))) {
								windowSize = node3.getText();
								map.put("windowSize", windowSize);
							}
							if ("stepSize".equals(node3
									.attributeValue("name"))) {
								stepSize = node3.getText();
								map.put("stepSize", stepSize);
							}
							if ("minSupport".equals(node3
									.attributeValue("name"))) {
								minSupport = node3.getText();
								map.put("minSupport", minSupport);
							}
						}
					}
				}
			}
			if ("datasets".equals(node2.getName())) {
				Iterator iter2 = node2.elementIterator();
				while (iter2.hasNext()) {
					Element node3 = (Element) iter2.next();
					if ("database".equals(node3.getName())) {
						if ("dbName".equals(node3.attributeValue("name"))) {
							dbName = node3.getText();
							map.put("dbName", dbName);
						}
					}
					if ("sourcetable".equals(node3.getName())) {
						if ("sourceTable".equals(node3.attributeValue("name"))) {
							sourceTable = node3.attributeValue("value");
							map.put("sourceTable", sourceTable);
							Iterator iter3 = node3.elementIterator();
							while (iter3.hasNext()) {
								Element node4 = (Element) iter3.next();
								if ("parameter".equals(node4.getName())) {
									if ("partition".equals(node4
											.attributeValue("name"))) {
										partition = node4.getText();
										map.put("partition", partition);
									}
									if ("timeFieldPos".equals(node4
											.attributeValue("name"))) {
										timeFieldPos = node4.getText();
										map.put("timeFieldPos", timeFieldPos);
									}
									if ("ipFieldPos".equals(node4
											.attributeValue("name"))) {
										ipFieldPos = node4.getText();
										map.put("ipFieldPos", ipFieldPos);
									}
									if ("messageFieldPos".equals(node4
											.attributeValue("name"))) {
										messageFieldPos = node4.getText();
										map.put("messageFieldPos",
												messageFieldPos);
									}
								}
							}
						}
					}
					if ("alarmtable".equals(node3.getName())) {
						if ("alarmTable".equals(node3.attributeValue("name"))) {
							alarmTable = node3.attributeValue("value");
							map.put("alarmTable", alarmTable);
							Iterator iter3 = node3.elementIterator();
							while (iter3.hasNext()) {
								Element node4 = (Element) iter3.next();
								if ("parameter".equals(node4.getName())) {
									if ("alarmTimeField".equals(node4
											.attributeValue("name"))) {
										alarmTimeField = node4.getText();
										map.put("alarmTimeField", alarmTimeField);
									}
									if ("alarmLabelField".equals(node4
											.attributeValue("name"))) {
										alarmLabelField = node4.getText();
										map.put("alarmLabelField", alarmLabelField);
									}
								}
							}
						}
					}
					if ("ipset".equals(node3.getName())) {
						if ("ips".equals(node3.attributeValue("name"))) {
							Iterator iter3 = node3.elementIterator();
							while (iter3.hasNext()) {
								Element node4 = (Element) iter3.next();
								if ("row".equals(node4.getName())) {
									ips.add(node4.getText());
									System.out.println(node4.getText());
								}
								ipMap.put("ips", ips);
							}
						}
					}
				}

			}
		}
		list.add(map);
		list.add(ipMap);
		return list;
	}
}