package com.reviewing.hive.udf;

import org.junit.Test;

/**
 * @author Rahul Desai, Suruchi Sharma, Nikita Amartya 
 * This is the jUnit test class for testing the Review UDF
 *         algorithm.
 * 
 */
 
public class ReviewWeightTest {

	@Test
	public void test() {
		String reviewText = "this is a very bad restaurant";
		String stars = "5";
	    Double result = ReviewWeight.evaluate(reviewText,stars);
	    
	    System.out.println(result);
	}

}
