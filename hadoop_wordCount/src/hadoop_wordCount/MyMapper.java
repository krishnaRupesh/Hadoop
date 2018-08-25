package hadoop_wordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	Text word = new Text();
	IntWritable one = new IntWritable(1);
	@Override
	public void map(LongWritable key, Text value, Context con) throws IOException,InterruptedException
	{
		StringTokenizer itr =new StringTokenizer(value.toString());
		
		while(itr.hasMoreElements()) {
			word.set(itr.nextToken());
			con.write(word, one);
			
		}	
	}
}
 