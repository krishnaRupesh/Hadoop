package hadoop.maxTemp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	Text keys =new Text();
	IntWritable values =new IntWritable();
	public void map(LongWritable byt, Text Value, Context con) throws IOException, InterruptedException {
		
		StringTokenizer str=new StringTokenizer(Value.toString());
		
		if(str.countTokens()==2) {
			String key= str.nextToken();
			int temp = Integer.parseInt(str.nextToken());
			keys.set(key);
			values.set(temp);
			con.write(keys, values);
			
		}
	}
	
}
