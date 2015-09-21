package com.cbc.extractor;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.cbc.extractor.UserDataMapper;
import com.cbc.extractor.UserDataReducer;

/**
 * @author Nikita, Suruchi, Rahul This Job extracts the used for extracting
 *         useful fields from user data from yelp which we get after json to
 *         psv. Our input is a set of records with individual fields separated
 *         by "|".
 */
public class UserDataJob {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: UserDataJob <input_dir_name> <output_dir_name>\n");
			System.exit(-1);
		}

		Job UserData_job = new Job();
		UserData_job.setJarByClass(UserDataJob.class);
		UserData_job.setJobName("UserData");

		final Configuration conf = UserData_job.getConfiguration();

		conf.set("mapred.textoutputformat.separator", "|");// to change the
															// separator from
															// tab to pipe

		FileInputFormat.addInputPath(UserData_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(UserData_job, new Path(args[1]));
		UserData_job.setMapperClass(UserDataMapper.class);
		UserData_job.setReducerClass(UserDataReducer.class);
		UserData_job.setOutputKeyClass(Text.class);
		UserData_job.setOutputValueClass(Text.class);
		System.exit(UserData_job.waitForCompletion(true) ? 0 : 1);
	}
}
