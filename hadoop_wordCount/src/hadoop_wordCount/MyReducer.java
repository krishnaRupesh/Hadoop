package hadoop_wordCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer< Text, IntWritable, Text,IntWritable>{

	IntWritable result=new IntWritable();
	
	public void reduce(Text key,Iterable<IntWritable> value ,Context con) throws IOException, InterruptedException {
		
		int sum=0;
		for(IntWritable r:value) {
			sum=sum+r.get();
		}
		result.set(sum);
		con.write(key, result);
		
	}
	
}
