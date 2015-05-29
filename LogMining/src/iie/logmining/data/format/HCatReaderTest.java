package iie.logmining.data.format;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;

/**
 * 利用HCatReader接口，从hive表里读数据，写到指定文件中
 * @author xiaodongfang
 *
 */
public class HCatReaderTest {

	public static void main(String[] args) throws IOException {

		String dbName = "xdf";
		String inputTableName = "logdata3";
		// String readerOutFile = "/user/xdf/streaming/testout.txt";
		// 设置输出表的名字
		String readerOutFile = "/user/xdf/streaming/log.txt";

		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = builder.withDatabase(dbName)
				.withTable(inputTableName).build();
		Map<String, String> config = new HashMap<String, String>();
		HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
		ReaderContext cntxt = reader.prepareRead();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(readerOutFile), conf);
		OutputStream out = fs.create(new Path(readerOutFile),
				new Progressable() {
					public void progress() {
					}
				});
		for (int i = 0; i < cntxt.numSplits(); ++i) {
			HCatReader splitReader = DataTransferFactory
					.getHCatReader(cntxt, i);
			Iterator<HCatRecord> itr1 = splitReader.read();
			while (itr1.hasNext()) {
				HCatRecord record = itr1.next();
				try {
					Iterator<Object> it2 = record.getAll().iterator();
					while (it2.hasNext()) {
						out.write(it2.next().toString().getBytes());
						if (it2.hasNext()) {
							out.write(';');
						} else {
							out.write('\n');
						}
					}
					out.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		out.close();
		System.out.println("生成文件的位置为：" + readerOutFile);
		System.exit(0);
	}
}
