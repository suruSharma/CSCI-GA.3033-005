package com.cbc.reviews;

/**
 * @author Nikita, Suruchi, Rahul 
 * Reducer for extracting useful fields from User Data 
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewsDataReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {
			context.write(key, value);
		}
	}
}
