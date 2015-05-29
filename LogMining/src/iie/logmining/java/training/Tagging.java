package iie.logmining.java.training;

import iie.logmining.java.training.COMMON_PATH;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Tagging {

	public static void main(String[] args) throws ParseException {
		long startTime = System.currentTimeMillis();
		COMMON_PATH.DELETE_FILE(COMMON_PATH.LABEL_DOCIDS_PATH);//写入label docIds文件前先删除原文件
		COMMON_PATH.DELETE_FILE(COMMON_PATH.LABEL_VECTOR_PATH);//写入label vector文件前先删除原文件
//		System.out.println("Saving dcoId Vector into Lucene...");
//		IndexWriter LLWriter = null;
//		try {
//			// save dcoId Vector
//			COMMON_PATH.INIT_DIR(COMMON_PATH.VECTOR_LUCENE_PATH);// 初始化Vector
//																	// Lucene文件夹
//			Directory LLDirectory = FSDirectory.open(new File(
//					COMMON_PATH.VECTOR_LUCENE_PATH));
//			IndexWriterConfig LLiwc = new IndexWriterConfig(
//					Version.LUCENE_4_10_2, new StandardAnalyzer());
//			LLiwc.setUseCompoundFile(false);
//			LLWriter = new IndexWriter(LLDirectory, LLiwc);
//			Document document = null;
//			double docCount = 0.0;
//			List<String> vectorList = new ArrayList<String>();
//			try {
//				System.out.println("Reading Vector.txt..." + new Date());
//				// File vectorFile = new File(COMMON_PATH.VECTOR_PATH);
//				File vectorFile = new File(COMMON_PATH.VECTOR_PATH);
//				BufferedReader vReader = new BufferedReader(
//						new InputStreamReader(new FileInputStream(vectorFile),
//								"UTF-8"));
//				String vLine = "";
//				int lineCount = 0;
//				long oneWStartTime = System.currentTimeMillis();
//				while ((vLine = vReader.readLine()) != null) {
//					lineCount++;
//					if ("".equals(vLine.trim()))
//						continue;
//					String[] arr = vLine.split("\t");
//					document = new Document();
//					document.add(new TextField("docId", arr[0], Field.Store.YES));
//					document.add(new StringField("vector", arr[1],
//							Field.Store.YES));
//					LLWriter.addDocument(document);
//					if (docCount % 1000000 == 0) {
//						System.out.println("process 100W records)，use "
//								+ (System.currentTimeMillis() - oneWStartTime)
//								/ 1000 + "S\n");
//						oneWStartTime = System.currentTimeMillis();
//					}
//				}
//				vReader.close();
//			} catch (IOException e1) {
//				e1.printStackTrace();
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			if (LLWriter != null) {
//				try {
//					LLWriter.close();
//				} catch (CorruptIndexException e) {
//					e.printStackTrace();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}

		System.out.println("Tagging...");

		Directory directory = null;
		IndexReader reader = null;
		try {
			directory = FSDirectory.open(new File(
					COMMON_PATH.VECTOR_LUCENE_PATH));
			reader = IndexReader.open(directory);
			IndexSearcher searcher = new IndexSearcher(reader);
			Terms msgTerm = MultiFields.getTerms(reader, "vector");
			TermsEnum msgEnum = msgTerm.iterator(null);
			int docId;
			int totalDocs = reader.maxDoc();
			int percent = (int) (totalDocs * 0.01 + 1);
			long percentStartTime = System.currentTimeMillis();
			int docCount = 0;
			int LVCount = 0;
			int LDCount = 0;
			while (msgEnum.next() != null) {
				String term = msgEnum.term().utf8ToString();
				// System.out.println(term);

				// 把Label Vector写入文件
				try {
					BufferedWriter LVWriter = new BufferedWriter(
							new FileWriter(new File(
									COMMON_PATH.LABEL_VECTOR_PATH), true));
					LVWriter.write("L" + (LVCount++) + "\t" + term);
					LVWriter.newLine();
					LVWriter.flush();
					LVWriter.close();
				} catch (Exception e) {
					e.printStackTrace();
				}

				DocsEnum termDocs = msgEnum.docs(null, null,
						DocsEnum.FLAG_FREQS);
				StringBuffer docIds = new StringBuffer();
				while ((docId = termDocs.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
					String docIdField = searcher.doc(docId).get("docId");
					docIds.append(docIdField + ",");
					if (++docCount % percent == 0) {
						System.out.println("Tagged:" + (double) docCount
								/ totalDocs * 100 + "%");
						System.out
								.println("process 10%("
										+ percent
										+ "records)，use "
										+ (System.currentTimeMillis() - percentStartTime)
										/ 1000 + "S\n");
						percentStartTime = System.currentTimeMillis();
					}
				}

				// 把Label docIds写入文件
				try {
					BufferedWriter DLWriter = new BufferedWriter(
							new FileWriter(new File(
									COMMON_PATH.LABEL_DOCIDS_PATH), true));
					DLWriter.write("L" + (LDCount++) + "\t"
							+ docIds.toString());
					DLWriter.newLine();
					DLWriter.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("Completed.Total process time:"
				+ (System.currentTimeMillis() - startTime) / 1000 + "S\n\n");
	}
}
