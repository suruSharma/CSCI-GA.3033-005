package wordCount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String lowerCase = line.toLowerCase();
		
		String hackathon = "hackathon";
		String dec = "Dec";
		String java = "Java";
		String chicago = "Chicago";
		
		
		context.write(new Text(hackathon), new IntWritable(lowerCase.contains(hackathon.toLowerCase())? 1:0));
		context.write(new Text(dec), new IntWritable(lowerCase.contains(dec.toLowerCase())? 1:0));
		context.write(new Text(chicago), new IntWritable(lowerCase.contains(chicago.toLowerCase())? 1:0));
		context.write(new Text(java), new IntWritable(lowerCase.contains(java.toLowerCase())? 1:0));
		
		
		
	}
	
}
