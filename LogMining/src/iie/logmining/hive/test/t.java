package iie.logmining.hive.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

public class t {

	public static void main(String[] args) throws IOException {
		
		String message ="smartd[7284]: Device: /dev/sda, [HITACHI HUS156030VLS600 A510], lu id: 0x5000cca041875450, S/N: CVXDE6SN, 300 GB";
		//String message ="v";
		final String regHexadecimal = "^[0-9a-fA-Fx]*$";// 16进制
		final String regNumber = "^[0-9.,]*$";// 10进制
		final String regNotation = "[\\.]";// 标点符号
		//final String regUnderLine = "[^_]*";// 下划线
		final String regStoreMemory = "(?!^[kmgb]*$)^([0-9kmgb.])*$";// 存储大小，不去除mb，gb
		final String regIP = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
		
		StandardAnalyzer analyzer;// luncene分词器
		
		List<String> sw = new LinkedList<String>();
		sw.add("");
		CharArraySet stopWords = new CharArraySet(sw, true);
		stopWords.add("");
		// 加入系统默认停用词SimpleAnalyzer
		Iterator<Object> itor = StandardAnalyzer.STOP_WORDS_SET
				.iterator();
		while (itor.hasNext()) {
			stopWords.add(itor.next());
		}
		analyzer = new StandardAnalyzer(stopWords);
		TokenStream ts = analyzer.tokenStream("field", message);
		CharTermAttribute ch = ts
				.addAttribute(CharTermAttribute.class);
		ts.reset();
		List<String> recordArr = new ArrayList<String>();
		while (ts.incrementToken()) {
			recordArr.add(ch.toString());
		}

		ts.end();
		ts.close();
		// 返回分词过滤后的message
		String reMessage = "";
		for (int j = 0; j < recordArr.size(); j++) {
			boolean isNumber = recordArr.get(j).matches(
					regNumber);
			boolean isIP = recordArr.get(j).matches(regIP);
			boolean isHexadecimal = recordArr.get(j).matches(
					regHexadecimal);
			boolean isStoreMemory = recordArr.get(j).matches(
					regStoreMemory);
//			boolean isNotUnderLine = recordArr.get(j).matches(
//					regUnderLine);&& (isNotUnderLine)

			if ((!isNumber) && (!isIP) && (!isHexadecimal)
					&& (!isStoreMemory) ) {
				
				String finalStr = recordArr.get(j);
				// 去除字符串末尾的数字和标点
				String temp = finalStr.substring(finalStr
						.length() - 1);
				boolean tailIsNumber = temp.matches(regNumber)||temp.matches(regNotation);
			
				String str = "";
				if (tailIsNumber) {
					for (int i = finalStr.length() - 1; i > 0; i--) {
						String tempStr = finalStr.substring(i);
						boolean isNumber2 = tempStr
								.matches(regNumber)||tempStr.matches(regNotation);
						str = finalStr.substring(0, i+1);
						if (!isNumber2)
							break;
						
					}
					if (j < recordArr.size() - 1) {
						reMessage += str + " ";
					}
					if (j == recordArr.size() - 1) {
						reMessage += str;
					}
				} else {
					if (j < recordArr.size() - 1) {
						reMessage += finalStr + " ";
					}
					if (j == recordArr.size() - 1) {
						reMessage += finalStr;
					}
				}
			}
		}
		System.out.println(reMessage);
	}
}
