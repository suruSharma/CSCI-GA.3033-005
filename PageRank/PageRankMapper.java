package pageRank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Suruchi Sharma 
 * N15950208 
 * sss665@nyu.edu
 * 
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String[] split = line.split(" ");

		int length = split.length;

		String source = split[0];

		String outLinks = "";

		float defaultPageRank = Float.parseFloat(split[length-1]);

		int numOfOutLinks = length - 2;

		float actualContribution = defaultPageRank / numOfOutLinks;

		

		for (int i = 1; i < length - 1; i++) {
			outLinks += split[i] + " ";
			context.write(new Text(split[i]), new Text(source + ","
					+ actualContribution));
		}
		
		context.write(new Text(source), new Text(outLinks));

	}

}
