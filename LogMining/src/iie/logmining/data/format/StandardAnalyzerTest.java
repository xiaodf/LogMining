package iie.logmining.data.format;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

public class StandardAnalyzerTest {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		try {
			// 要处理的文本
            String text = "ipc,ipc,server,handler,org.apache.hadoop.mapred.taskattemptlistenerimpl,status,update,from,attempt_1407835338302_0022_m_000472_0";
			// 自定义停用词		
			List<String> sw = new LinkedList<String>();// custom stopWords set
			sw.add("");
			CharArraySet stopWords = new CharArraySet(sw, true);	
			stopWords.add("_");
			// 加入系统默认停用词
			Iterator<Object> itor = StandardAnalyzer.STOP_WORDS_SET.iterator();
			while (itor.hasNext()) {
				stopWords.add(itor.next());
			}
			// 标准分词器(Lucene内置的标准分析器,会将语汇单元转成小写形式，并去除停用词及标点符号)
			StandardAnalyzer analyzer = new StandardAnalyzer(stopWords);
			TokenStream ts = analyzer.tokenStream("field", text);
			CharTermAttribute ch = ts.addAttribute(CharTermAttribute.class);
			ts.reset();
			while (ts.incrementToken()) {
				System.out.println(ch.toString());
			}
			ts.end();
			ts.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
