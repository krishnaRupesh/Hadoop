package hadoop.maxTemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	Text keys =new Text();
	IntWritable maxs=new IntWritable();
	public void reduce(Text key,Iterable<IntWritable> value, Context con) throws IOException, InterruptedException {
		
		int max = 0;
		for (IntWritable v: value) {
			
			if(v.get()>max) {
				
				max=v.get();
			}
		}
		keys.set(key);
		maxs.set(max);
		con.write(keys, maxs);
	}
}
