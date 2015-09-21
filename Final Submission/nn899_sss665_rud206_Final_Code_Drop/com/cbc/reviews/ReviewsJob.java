package com.cbc.reviews;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.cbc.reviews.ReviewsDataMapper;
import com.cbc.reviews.ReviewsDataReducer;

/**
 * @author Nikita, Suruchi, Rahul This Job extracts the used for extracting
 *         useful fields from reviews from yelp which we get after json to psv.
 *         Our input is a set of records with individual fields separated by
 *         "|".
 */
public class ReviewsJob {
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: ReviewsJob <input_dir_name> <output_dir_name>\n");
			System.exit(-1);
		}

		Job reviewsDataJob = new Job();
		reviewsDataJob.setJarByClass(ReviewsJob.class);
		reviewsDataJob.setJobName("reviewsData");

		final Configuration conf = reviewsDataJob.getConfiguration();

		conf.set("mapred.textoutputformat.separator", "|");// to change the
															// separator from
															// tab to pipe

		FileInputFormat.addInputPath(reviewsDataJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(reviewsDataJob, new Path(args[1]));
		reviewsDataJob.setMapperClass(ReviewsDataMapper.class);
		reviewsDataJob.setReducerClass(ReviewsDataReducer.class);
		reviewsDataJob.setOutputKeyClass(Text.class);
		reviewsDataJob.setOutputValueClass(Text.class);
		System.exit(reviewsDataJob.waitForCompletion(true) ? 0 : 1);
	}
}
