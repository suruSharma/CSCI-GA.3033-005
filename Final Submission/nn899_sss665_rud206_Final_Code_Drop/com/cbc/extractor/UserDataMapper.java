package com.cbc.extractor;

/**
 * @author Nikita, Suruchi, Rahul 
 * Mapper for extracting useful fields from User Data 
 * Fields that we will use for our purpose are :
 * reviewCount, 
 * averageStars,
 * userName,
 * userId,
 * votesFunny,
 * votesCool,
 * votesUseful 
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		ArrayList<Integer> colCount = new ArrayList<Integer>();
		colCount.add(2);
		colCount.add(12);
		colCount.add(15);
		colCount.add(16);
		colCount.add(17);
		colCount.add(19);
		colCount.add(22);		

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
