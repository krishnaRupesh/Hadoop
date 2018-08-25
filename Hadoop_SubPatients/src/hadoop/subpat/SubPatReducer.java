package hadoop.subpat;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SubPatReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	IntWritable result = new IntWritable();
	public void reduce(IntWritable key,Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
		
		int sum=0;
		for(IntWritable r: values) {
			sum=sum+r.get();
		}
		result.set(sum);
		con.write(key, result);
	}
}
