package com.cbc.reviews;


/**
 * @author Nikita, Suruchi, Rahul 
 * Mapper for extracting useful fields from Reviews
 * Fields that we will use for our purpose are :
 * userId, 
 * reviewId,
 * reviewText,
 * votesCool,
 * businessID,
 * votesFunny,
 * stars,
 * reviewDate,
 * votesUseful
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewsDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		ArrayList<Integer> colCount = new ArrayList<Integer>();
		colCount.add(0);
		colCount.add(1);
		colCount.add(2);
		colCount.add(3);
		colCount.add(4);
		colCount.add(5);
		colCount.add(6);
		colCount.add(7);
		colCount.add(9);
		

		StringBuffer sb = new StringBuffer("");
		String userId = line.split("\\|")[0];
		for(int col:colCount)
		{
			sb.append(line.split("\\|")[col]);
			sb.append("|");
		}
		sb.setLength(sb.length()-1);//to remove the last pipe
		context.write(new Text(userId), new Text(sb.toString()));
	}
}
