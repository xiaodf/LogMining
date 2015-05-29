package iie.logmining.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.SparkConf;
import com.google.common.base.Joiner;

public class TextSearch {
	static double minsup = 0.5;
	static double minconf = 0.5;

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("usage:TextSearch [minsup] [minconf]");
			return;
		}
		minsup = Double.parseDouble(args[0]);
		minconf = Double.parseDouble(args[1]);
		SparkConf conf = new SparkConf().setAppName("textsearch");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// 读取文件
		JavaRDD<String> lines = jsc.textFile("/user/xdf/item.txt");
		Long count = lines.count();
		// Function的参数：第一个是输入类型；第二个是返回类型
		JavaRDD<List<String>> transactions = lines
				.map(new Function<String, List<String>>() {
					private static final long serialVersionUID = 1L;

					public List<String> call(String arg0) throws Exception {
						List<String> list = new ArrayList<String>();
						String[] s = arg0.split(",");
						for (int i = 0; i < s.length; i++) {
							list.add(s[i]);
						}
						return list;
					}
				});
		// FPGrowth频繁项集挖掘
		FPGrowth fpg = new FPGrowth().setMinSupport(minsup);
		FPGrowthModel<String> model = fpg.run(transactions);

		/*
		 * 获得频繁项集和频率, 记录频繁项集的时候，首先用set记录一下每个频繁项中的元素并按照set默认的排序规则进行排序，
		 * 这样频繁项集中的元素就都是按照同一种排序规则进行排列的了
		 */
		Set<String> freqitem = new HashSet<String>();
		Map<String, Long> freq = new HashMap<String, Long>();
		for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets()
				.toJavaRDD().collect()) {
			System.out.println("[" + Joiner.on(",").join(itemset.javaItems())
					+ "], " + itemset.freq());
			for (String s : itemset.javaItems()) {// 用set把元素按照默认的规则进行排序处理
				freqitem.add(s);
			}
			// 用freq记录频繁项集
			freq.put(Joiner.on(",").join(freqitem), itemset.freq());
			freqitem.clear();
		}

//		System.out.println("check if it's sorted");
//		for (String key : freq.keySet()) {
//			System.out.println(key);
//		}

		// 获得关联规则
		List<Item> rules = new ArrayList<Item>();
		// 对每一个k频繁项，分别求1->k-1,2->k-2.....k-1->1的关联规则
		for (String key : freq.keySet()) {
			rules.addAll(subGen(key, freq, count));
		}
		// 输出关联规则
		System.out.println("rules:-------------" + rules.size());
		for (Item item : rules) {
			System.out.println(item.getLfs() + "->" + item.getRfs() + "\t"
					+ item.getSupport() + "\t" + item.getConfidence() + "\t"
					+ item.getLift());
		}

		jsc.stop();
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
//			System.out.println("test");
//			System.out.println("x.size()=" + x.size());
//			System.out.println(freqcurrent);
//			System.out.println(freq.get(lfs));
			double confidence = (double) freqcurrent / freq.get(lfs);
			//System.out.println(confidence);
			if (confidence >= minconf) {
				double support = (double) freqcurrent / count;
				double lift = (double) confidence / freq.get(rfs) * count;
//				System.out.println("inside:" + lfs + "->" + rfs + "\t"
//						+ support + "\t" + confidence + "\t" + lift);
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
