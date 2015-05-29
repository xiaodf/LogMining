package iie.udps.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

public class JavaExample {

	public static void main(String[] args) throws HCatException {
		String dbName = "xdf";
		String inputTable = args[0];
		String outputTable = args[0];
		int field = 1;

		// 从hive表中读出数据
		ReaderContext context = getContext(dbName, inputTable);
		// 向hive表中写入数据
		storeToTable(dbName, outputTable, context, field);
	}

	/**
	 * 将处理后的数据存到输出表中
	 * 
	 * @param dbName
	 * @param featureTable
	 * @throws HCatException
	 */
	public static void storeToTable(String dbName, String featureTable,
			ReaderContext data, int field) throws HCatException {
		// 数据存到hive表中
		WriteEntity.Builder builder = new WriteEntity.Builder();
		WriteEntity entity = builder.withDatabase(dbName)
				.withTable(featureTable).build();
		Map<String, String> config = new HashMap<String, String>();
		HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
		WriterContext context = writer.prepareWrite();
		HCatWriter splitWriter = DataTransferFactory.getHCatWriter(context);
		List<HCatRecord> records = new ArrayList<HCatRecord>();

		for (int i = 0; i < data.numSplits(); i++) {

			HCatReader splitReader = DataTransferFactory.getHCatReader(data, i);
			Iterator<HCatRecord> itr1 = splitReader.read();
			List<Object> temp = new ArrayList<Object>(2);
			while (itr1.hasNext()) {
				HCatRecord record = itr1.next();
				Iterator<Object> it2 = record.getAll().iterator();
				while (it2.hasNext()) {
					temp.add(it2.next());
				}
			}
			// 输出数据对象
			records.add(new DefaultHCatRecord(temp));
		}
		splitWriter.write(records.iterator());
		writer.commit(context);
	}

	/**
	 * 读取hive表中数据
	 * 
	 * @param dbName
	 * @param inputTable
	 * @return
	 * @throws HCatException
	 */
	public static ReaderContext getContext(String dbName, String inputTable)
			throws HCatException {
		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = builder.withDatabase(dbName).withTable(inputTable)
				.build();
		Map<String, String> config = new HashMap<String, String>();
		HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
		ReaderContext cntxt = reader.prepareRead();
		return cntxt;
	}
}
