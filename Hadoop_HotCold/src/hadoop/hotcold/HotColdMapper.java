package hadoop.hotcold;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HotColdMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	LongWritable key = new LongWritable();
	Text Value = new Text();

	public void map(LongWritable bit, Text input, Context con) throws IOException, InterruptedException {

		String record = input.toString();
		String[] parts = record.split("\\s+");

		if (parts.length > 2) {

			key.set(Long.parseLong(parts[1]));

			String str = null;
			if (Double.parseDouble(parts[5]) > 40.0) {
				str = "HotDay";

			} else if (Double.parseDouble(parts[6]) < 10.0) {
				str = "ColdDay";

			}
			if (str != null) {
				Value.set(str);
				con.write(key, Value);
			}
		}
	}
}
