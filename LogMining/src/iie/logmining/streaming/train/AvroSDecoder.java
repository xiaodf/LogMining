package iie.logmining.streaming.train;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import kafka.utils.VerifiableProperties;

public class AvroSDecoder implements kafka.serializer.Decoder<String[]> {

	private DatumReader<GenericRecord> reader;

	public AvroSDecoder(VerifiableProperties props) {

		// TODO Auto-generated constructor stub

		String schema = props.getString("KafkaTopicSchema");
		reader = new GenericDatumReader<GenericRecord>(
				new Schema.Parser().parse(schema));

	}

	@Override
	public String[] fromBytes(byte[] arg0) {
		// TODO Auto-generated method stub
		Decoder decoder = DecoderFactory.get().binaryDecoder(arg0, null);
		GenericRecord record = null;
		try {
			record = reader.read(null, decoder);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		int fieldSize = record.getSchema().getFields().size();

		String[] desline = new String[fieldSize];
		for (int i = 0; i < fieldSize; i++) {
			desline[i] = record.get(i).toString();
		}
		return desline;
	}

}