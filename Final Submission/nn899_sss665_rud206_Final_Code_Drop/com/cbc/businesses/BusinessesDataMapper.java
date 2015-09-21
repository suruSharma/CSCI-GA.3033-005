package com.cbc.businesses;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessesDataMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String MatchToString = new String("restaurant");
		String inputString = value.toString(); // input

		if (inputString.toLowerCase().contains(MatchToString)) {
			ArrayList<Integer> colCount = new ArrayList<Integer>();
			// We will keep column ->
			// 1-5,9-27,30-38,40-48,50-76,78-97,99-102,104,105
			// Not required -> 0,6,7,8,28,29,39,49,77,98,103
			int array[] = { 6, 7, 8, 28, 29, 39, 49, 77, 98, 103 };
			for (int toAdd = 1; toAdd < 106; toAdd++)
				colCount.add(toAdd);
			for (int toRemove = 0; toRemove < array.length; toRemove++)
				colCount.remove(toRemove);
			StringBuffer sb = new StringBuffer("");
			String userId = inputString.split("\\|")[0];
			for (int col : colCount) {
				sb.append(inputString.split("\\|")[col]);
				sb.append("|");
			}
			sb.setLength(sb.length() - 1);// to remove the last pipe
			context.write(new Text(userId), new Text(sb.toString()));

		}

	}

}
