package iie.logmining.example;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	public static void main(String[] args) {
		
		List<List<String>> list = new ArrayList<List<String>>();
	
		
		List<String> list1 = new ArrayList<String>();
		list1.add("2014-07-31 15:14:39");
		list1.add("4");
		list1.add("192.168.8.3");
		List<String> list2 = new ArrayList<String>();
		list2.add("2014-07-31 15:14:39");
		list2.add("4");
		list2.add("192.168.8.3");
		list.add(list1);
		list.add(list2);
		for(int i =0;i<list.size() ;i++){
			System.out.println(list.get(i));
		}
		
	}
}
