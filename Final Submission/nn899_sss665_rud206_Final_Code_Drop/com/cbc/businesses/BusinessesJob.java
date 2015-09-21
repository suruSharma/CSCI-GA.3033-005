package com.cbc.businesses;

/**
 * @author Nikita, Suruchi, Rahul 
 * This Job extracts the used for extracting useful fields from
 * businesses data from yelp which we get after json to psv conversion.
 * Our input is a set of records with individual fields 
 * separated by "|".
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cbc.businesses.BusinessesDataMapper;
import com.cbc.businesses.BusinessesDataReducer;
import com.cbc.businesses.BusinessesJob;

public class BusinessesJob {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: BusinessesJob <input_dir_name> <output_dir_name>\n");
			System.exit(-1);
		}

		Job businessesDataJob = new Job();
		businessesDataJob.setJarByClass(BusinessesJob.class);
		businessesDataJob.setJobName("businessesData");

		final Configuration conf = businessesDataJob.getConfiguration();

		conf.set("mapred.textoutputformat.separator", "|");// to change the
															// separator from
															// tab to pipe

		FileInputFormat.addInputPath(businessesDataJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(businessesDataJob, new Path(args[1]));
		businessesDataJob.setMapperClass(BusinessesDataMapper.class);
		businessesDataJob.setReducerClass(BusinessesDataReducer.class);
		businessesDataJob.setOutputKeyClass(Text.class);
		businessesDataJob.setOutputValueClass(Text.class);
		System.exit(businessesDataJob.waitForCompletion(true) ? 0 : 1);
	}

}
