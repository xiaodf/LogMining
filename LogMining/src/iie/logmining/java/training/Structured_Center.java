package iie.logmining.java.training;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class Structured_Center {

	public static void main(String[] args) throws Exception {
		System.out.println("Structured Running..." + new Date());
		long startTime = System.currentTimeMillis();
		IndexWriter writer = null;
		int logCount = 0;// 统计日志总条数
		try {
			List<String> sw = new LinkedList<String>();// custom stopWords set
			sw.add("");
			CharArraySet stopWords = new CharArraySet(sw, true);
			StandardAnalyzer analyzer = new StandardAnalyzer(stopWords);

			COMMON_PATH.INIT_DIR(COMMON_PATH.LUCENE_PATH);// 初始化Lucene文件夹
			Directory directory = FSDirectory.open(new File(
					COMMON_PATH.LUCENE_PATH));
			IndexWriterConfig iwc = new IndexWriterConfig(
					Version.LUCENE_4_10_2, analyzer);
			iwc.setUseCompoundFile(false);
			writer = new IndexWriter(directory, iwc);
			Document document = null;
			List<String> list = new ArrayList<String>();
			File f = new File(COMMON_PATH.RAW_LOG_FILE_PATH);
			File[] fileList = f.listFiles();
			double fileTotal = fileList.length;
			double fileCount = 0;
			for (File file : fileList) {
				list = getRealDataContent(file);
				logCount += list.size();
				System.out.println(logCount);
				for (int i = 0; i < list.size(); i++) {
					String[] recordArr = list.get(i).split(";| ");
					// 过滤不符合规则数据
					String regTimeDay = "^(2014-[0-1]?[0-9]-[0-3]?[0-9])$";
					String regTimeSec = "^([0-9][0-9]:[0-9][0-9]:[0-9][0-9])$";
					String regSegment = "BJLTSH-503-DFA-CL-SEV\\d+";
					if (!recordArr[3].matches(regTimeDay)) {
						continue;
					}
					if (!recordArr[4].matches(regTimeSec)) {
						continue;
					}
					if (!recordArr[5].matches(regSegment)) {
						continue;
					}

					document = new Document();
					document.add(new TextField("serviceName", recordArr[0],
							Field.Store.YES));
					document.add(new TextField("netType", recordArr[1],
							Field.Store.YES));
					document.add(new Field("ip", recordArr[2], Field.Store.YES,
							Field.Index.ANALYZED));
					document.add(new Field("timeStamp", recordArr[3] + " "
							+ recordArr[4], Field.Store.YES,
							Field.Index.ANALYZED));
					document.add(new TextField("segment", recordArr[5],
							Field.Store.YES));
					String source = "";
					String message = "";
					for (int k = 6; k < recordArr.length; k++) {
						message += recordArr[k] + " ";
					}
					String[] messArr = message.split(":");
					if (messArr.length > 1) {
						source = messArr[0];
						message = "";
						for (int j = 1; j < messArr.length; j++)
							message += messArr[j] + " ";
					}
					document.add(new TextField("source", source,
							Field.Store.YES));
					document.add(new Field("message", message, Field.Store.YES,
							Field.Index.ANALYZED,
							Field.TermVector.WITH_POSITIONS_OFFSETS));
					writer.addDocument(document);
				}
				System.out.println("Structured:"
						+ ((++fileCount) / fileTotal * 100) + "%");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (CorruptIndexException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		try {// 把统计结果写入统计汇总文件
			COMMON_PATH.DELETE_FILE(COMMON_PATH.STATISTICS_PATH);// 写入前，删除原统计文件
			BufferedWriter sWriter = new BufferedWriter(new FileWriter(
					new File(COMMON_PATH.STATISTICS_PATH), true));
			sWriter.write("***Structured    " + System.currentTimeMillis()
					+ "***");
			sWriter.newLine();
			sWriter.write("总日志条数:" + logCount);
			sWriter.newLine();
			sWriter.newLine();
			sWriter.flush();
			sWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Completed." + new Date() + "\n" + "process time:"
				+ (System.currentTimeMillis() - startTime) / 1000 + "S\n\n");
	}

	// 读日志文件
	public static List<String> getRealDataContent(File file) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(file), "GBK"));
		String tempLine = "";
		String curLine = br.readLine();
		String nextLine = "";
		List<String> contents = new ArrayList<String>();
		while (curLine != null) {
			contents.add(curLine);
			curLine = br.readLine();
		}
		br.close();
		return contents;
	}

}