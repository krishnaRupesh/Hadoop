package hadoop.alfaCount;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AlfaCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	
	private static final Logger logger = Logger.getLogger(AlfaCountReducer.class.getName());
	public void reduce(IntWritable size,Iterable<IntWritable> value, Context con) throws IOException, InterruptedException {
		IntWritable total = new IntWritable();
		int count=0;
		for(IntWritable w:value) {
			count++;
			//System.out.println("in_For"+count);
			logger.info("Logging inside..."+count);
		}
		//System.out.println("After_for"+count);
		logger.info("Logging outside..."+count);
		total.set(count);
		con.write(size, total);
		
	}

}
