package hadoop.subpat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SubPatMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	IntWritable intkey =new IntWritable();
	IntWritable one= new IntWritable(1);
	String OutKey,OutValue;
	public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException {
		
		StringTokenizer str=new StringTokenizer(value.toString());
		
		if (str.countTokens()==2) {
			
			OutKey= str.nextToken();
			int a =Integer.parseInt(OutKey);
			
				intkey.set(a);
			
		}	
			con.write(intkey, one);
		}
}
