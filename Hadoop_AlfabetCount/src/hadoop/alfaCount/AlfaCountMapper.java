package hadoop.alfaCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AlfaCountMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {
	
	IntWritable num=new IntWritable();
	IntWritable one = new IntWritable(1);
	@Override
	public void map(LongWritable key,Text value,Context con) throws  IOException,InterruptedException{
		
		StringTokenizer stkn=new StringTokenizer(value.toString());
		
		while(stkn.hasMoreElements()) {
			String str=stkn.nextToken();
			char[] k=str.toCharArray();
			num.set(k.length);
			con.write(num, one);
			
		}
		
	}
}
