package pageRank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Suruchi Sharma
 * N15950208
 * sss665@nyu.edu
 *
 */
public class PageRankReducer extends
		Reducer<Text, Text, Text, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		float pageRank = 0.0f;
		String finalValue = "";
		for (Text value : values) {
			String val = value.toString();
			if(val.contains(","))
			{
				float pr = Float.parseFloat((val.split(","))[1]);
				pageRank+=pr;	
			}
			else{
				finalValue+=val;
			}
		}
		
		finalValue+=pageRank;
		
		context.write(key, new Text(finalValue));
	}
}
