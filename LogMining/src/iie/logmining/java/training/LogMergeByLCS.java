package iie.logmining.java.training;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class LogMergeByLCS {
	public static double SIMILARITY = 0.5;
	public static List<String[]> LABEL_VECTOR_LIST = new ArrayList<String[]>();
	public static HashMap<String, String> LABEL_DOCIDS_MAP = new HashMap<String, String>();
	public static int LCS = 0;
	public static String SIMILAR_LABELS = "";
	public static List<String> LABEL_SET_LIST = new LinkedList<String>();
	public static boolean[] FLAG;

	static {
		// 读Label vector文件
		try {
			File LabelVectorFile = new File(COMMON_PATH.LABEL_VECTOR_PATH);
			BufferedReader vReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(LabelVectorFile), "UTF-8"));
			String line = vReader.readLine();
			while (line != null) {
				if ("".equals(line.trim())) {
					line = vReader.readLine();
					continue;
				}
				String[] labelVectorArr = line.split("\t|,");
				LABEL_VECTOR_LIST.add(labelVectorArr);
				line = vReader.readLine();
			}
			vReader.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// 读label docIds文件
		try {
			File LabelDocIDsFile = new File(COMMON_PATH.LABEL_DOCIDS_PATH);
			BufferedReader dReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(LabelDocIDsFile), "UTF-8"));
			String line = dReader.readLine();
			while (line != null) {
				if ("".equals(line.trim())) {
					line = dReader.readLine();
					continue;
				}
				String[] labelDocIdsArr = line.split("\t");
				LABEL_DOCIDS_MAP.put(labelDocIdsArr[0], labelDocIdsArr[1]);
				line = dReader.readLine();
			}
			dReader.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		System.out.println("LogMergeByLCS Running..." + new Date());
		int[][] LCSArr = new int[LABEL_VECTOR_LIST.size()][LABEL_VECTOR_LIST
				.size()];
		for (int i = 0; i < LABEL_VECTOR_LIST.size(); i++) {
			String[] rowStr = LABEL_VECTOR_LIST.get(i);
			for (int j = 0; j < LABEL_VECTOR_LIST.size(); j++) {
				String[] colStr = LABEL_VECTOR_LIST.get(j);
				LCS = LCSLength(rowStr, colStr);
				double rowStr_lcs = (double) LCS / (rowStr.length - 1);// 因为数组第一个是Label，计算长度时就不计算label，并且该算法也会忽略第一个字符。
				double colStr_lcs = (double) LCS / (colStr.length - 1);
				if (rowStr_lcs >= SIMILARITY && colStr_lcs >= SIMILARITY) {
					LCSArr[i][j] = 1;
				} else {
					LCSArr[i][j] = 0;
				}
			}
		}
		String[] vertexs = new String[LABEL_VECTOR_LIST.size()];
		for (int i = 0; i < LABEL_VECTOR_LIST.size(); i++) {
			String[] LVArr = LABEL_VECTOR_LIST.get(i);
			vertexs[i] = LVArr[0];
		}
		
		DFSTraverse(LCSArr.length, vertexs, LCSArr);

		// 把Label Set写入文件
		Directory directory = null;
		IndexReader reader = null;
		COMMON_PATH.DELETE_FILE(COMMON_PATH.LABEL_SET_PATH);// 写入Label_Set文件前先删除原文件

		try {
			directory = FSDirectory.open(new File(COMMON_PATH.LUCENE_PATH));
			reader = IndexReader.open(directory);
			Document document = null;
			int maxDoc = reader.maxDoc();
			System.out.println(maxDoc);
			System.out.println("Writing Label Set ...");
			try {
				BufferedWriter writer = new BufferedWriter(new FileWriter(
						new File(COMMON_PATH.LABEL_SET_PATH), true));

				for (int i = 0; i < LABEL_SET_LIST.size(); i++) {
					writer.write("===========Label_" + i + "===============");
					writer.newLine();
					writer.write(LABEL_SET_LIST.get(i));
					writer.newLine();
					writer.newLine();
					writer.flush();
				}
				writer.write("***************************");
				writer.newLine();
				writer.newLine();
				int labelSetCount = 0;
				for (int i = 0; i < LABEL_SET_LIST.size(); i++) {
					String[] labelArr = LABEL_SET_LIST.get(i).split(",");
					writer.write("===========Label_" + i + "===============");
					writer.newLine();
					for (int j = 0; j < labelArr.length; j++) {
						String docIds = LABEL_DOCIDS_MAP.get(labelArr[j]);
						String[] docIdArr = docIds.split(",");
						for (int k = 0; k < docIdArr.length; k++) {
							document = reader.document(Integer
									.valueOf(docIdArr[k]));
							// Message source写入文件
							writer.write("MESSAGE:" + document.get("message")
									+ " <-- SOURCE:" + document.get("source"));
							writer.newLine();
							writer.flush();
							if (labelSetCount++ % ((int) maxDoc * 0.1) == 0)
								System.out.println("saving labelSet "
										+ ((double) (labelSetCount / maxDoc))
										* 100 + "%");
						}
					}

				}
				writer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			System.out.println("Saving as Lucene File...");

			IndexWriter LLWriter = null;
			try {
				// save as Labeled Lucene File
				COMMON_PATH.INIT_DIR(COMMON_PATH.LABELED_LUCENE_PATH);// 初始化Labeled_Lucene文件夹
				Directory LLDirectory = FSDirectory.open(new File(
						COMMON_PATH.LABELED_LUCENE_PATH));
				IndexWriterConfig LLiwc = new IndexWriterConfig(
						Version.LUCENE_4_10_2, new StandardAnalyzer());
				LLiwc.setUseCompoundFile(false);
				LLWriter = new IndexWriter(LLDirectory, LLiwc);
				Document LLDocument = null;
				int docCount = 0;
				for (int i = 0; i < LABEL_SET_LIST.size(); i++) {
					double listSize = LABEL_SET_LIST.size();
					String[] labelArr = LABEL_SET_LIST.get(i).split(",");
					for (int j = 0; j < labelArr.length; j++) {
						String docIds = LABEL_DOCIDS_MAP.get(labelArr[j]);
						String[] docIdArr = docIds.split(",");
						for (int k = 0; k < docIdArr.length; k++) {
							document = reader.document(Integer
									.valueOf(docIdArr[k]));
							document.add(new Field("label", "Label_" + i,
									Field.Store.YES, Field.Index.NOT_ANALYZED));
							LLWriter.addDocument(document);
							++docCount;
							if (docCount % ((int) maxDoc * 0.05) == 0)
								System.out.println("saving Final Lucene "
										+ ((double) docCount / maxDoc) * 100
										+ "%");
						}
					}

				}

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (LLWriter != null) {
					try {
						LLWriter.close();
					} catch (CorruptIndexException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			// TimesTamp_Label文件
			// 原来在日志合并时用到，现在省掉这一文件，日志合并时直接从Lucene中读取。此时（2015-01-27）日志合并处还没改
			// // 写入TimesTamp_Label文件
			// COMMON_PATH.DELETE_FILE(COMMON_PATH.TIMESTAMP_LABEL_PATH);//
			// 写入TimesTamp_Label文件前先删除原文件
			// try {
			// BufferedWriter tlWriter = new BufferedWriter(new FileWriter(
			// new File(COMMON_PATH.TIMESTAMP_LABEL_PATH), true));
			// for (int i = 0; i < LABEL_SET_LIST.size(); i++) {
			// String[] labelArr = LABEL_SET_LIST.get(i).split(",");
			// for (int j = 0; j < labelArr.length; j++) {
			// String docIds = LABEL_DOCIDS_MAP.get(labelArr[j]);
			// String[] docIdArr = docIds.split(",");
			// for (int k = 0; k < docIdArr.length; k++) {
			// document = reader.document(Integer
			// .valueOf(docIdArr[k]));
			// // Message source写入文件
			// tlWriter.write(document.get("timeStamp") + "\t");
			// tlWriter.write("Label_" + i);
			// tlWriter.newLine();
			// }
			// }
			// }
			// tlWriter.flush();
			// tlWriter.close();
			// } catch (Exception e) {
			// e.printStackTrace();
			// }
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Writing Label docIds...");

		// 把Label docIds写入文件
		COMMON_PATH.DELETE_FILE(COMMON_PATH.LABEL_SET_DOCIDS_PATH);// 写入Label
																	// docIds文件前先删除原文件
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
					COMMON_PATH.LABEL_SET_DOCIDS_PATH), true));

			for (int i = 0; i < LABEL_SET_LIST.size(); i++) {
				String[] labelsArr = LABEL_SET_LIST.get(i).split(",");
				String docIds = "";
				for (int j = 0; j < labelsArr.length; j++) {
					docIds += LABEL_DOCIDS_MAP.get(labelsArr[j]);
				}
				writer.write("Label_" + i + "\t" + docIds);
				writer.newLine();
				writer.newLine();
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Completed." + new Date() + "\n\n");
	}

	// 计算最长公共子串长度
	public static int LCSLength(String[] x, String[] y) {
		// int[][] b = new
		// int[x.length][y.length];//b[][]存储x、yLCS路径，这里只需要长度，不需要得到LCS内容
		int[][] c = new int[x.length][y.length];// c[][]存储x、yLCS长度
		for (int i = 1; i < x.length; i++) {
			for (int j = 1; j < y.length; j++) {
				if (x[i].equals(y[j])) {
					c[i][j] = c[i - 1][j - 1] + 1;
				} else if (c[i - 1][j] >= c[i][j - 1]) {
					c[i][j] = c[i - 1][j];
				} else {
					c[i][j] = c[i][j - 1];
				}
			}
		}
		return c[x.length - 1][y.length - 1];
	}

	// 图的深度遍历操作(递归)
	public static void DFSTraverse(int nodeCount, String[] vertexs,
			int[][] edges) {
		FLAG = new boolean[nodeCount];
		for (int i = 0; i < nodeCount; i++) {
			SIMILAR_LABELS = "";
			if (FLAG[i] == false) {// 当前顶点没有被访问
				DFS(i, nodeCount, vertexs, edges);
				LABEL_SET_LIST.add(SIMILAR_LABELS);
			}
		}
	}

	// 图的深度优先递归算法
	public static void DFS(int i, int nodeCount, String[] vertexs, int[][] edges) {
		FLAG[i] = true;// 第i个顶点被访问
		SIMILAR_LABELS += vertexs[i] + ",";
		for (int j = 0; j < nodeCount; j++) {
			if (FLAG[j] == false && edges[i][j] == 1) {
				DFS(j, nodeCount, vertexs, edges);
			}
		}
	}
}
