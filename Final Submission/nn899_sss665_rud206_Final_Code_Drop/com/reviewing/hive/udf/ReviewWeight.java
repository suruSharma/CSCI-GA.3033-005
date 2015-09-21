package com.reviewing.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.Vector;

/**
 * @author Rahul Desai, Suruchi Sharma, Nikita Amartya The UDF for calculating the weighted
 *         review for the reviews table. The evaluate method takes the textual
 *         review and the numerical review as input and processed it using NLP
 *         algorithm.
 * 
 */
public final class ReviewWeight extends UDF {
	public static double evaluate(final String textualReview, final String numericalReview) {
		String st = numericalReview == null || numericalReview.equals("") ? "2.5" : numericalReview;
		double stars = Double.parseDouble(st);
		double basevalue;

		double bvalue[] = { 10, 18, 26, 34, 42, 50, 58, 66, 74, 82, 90 };
		basevalue = bvalue[(int) (stars * 2)];
		WordList words = new WordList();
		int poscount = 0;
		int negcount = 0;
		String[] tokens = textualReview.split(" ");
		for (int i = 0; i < tokens.length; i++) {
			String token = tokens[i];
			if (token.length() > 1) {
				String tokenSubString = token.substring(0, 1);
				int count = 1;
				while (!tokenSubString.equalsIgnoreCase(token)
						&& (count <= token.length())) {
					if (words.pos.contains(tokenSubString.toLowerCase())) {
						poscount++;
					}
					if (words.neg.contains(tokenSubString.toLowerCase())) {
						negcount++;
					}
					count++;
					tokenSubString = token.substring(0, count);
				}
				if (words.pos.contains(tokenSubString.toLowerCase())) {
					poscount++;
				}
				if (words.neg.contains(tokenSubString.toLowerCase())) {
					negcount++;
				}
			}

		}
		double weight = (10.0 / (poscount + negcount));
		if (weight > 2.0) {
			weight = 2.0;
		}
		basevalue = basevalue + (weight * poscount) - (weight * negcount);
		return basevalue;
	}
}

class WordList {
	Vector<String> pos = new Vector<String>();
	Vector<String> neg = new Vector<String>();

	public WordList() {
		// the add method of vector class adds the element at the end of the
		// vector
		// one word is only added once; no duplicates

		// adding positive words
		pos.add("amazing");
		pos.add("awesome");
		pos.add("afford");
		pos.add("affordable");
		pos.add("best");
		pos.add("better");
		pos.add("big");
		pos.add("correct");
		pos.add("crave");
		pos.add("comfortable");
		pos.add("crisp");
		pos.add("crispy");
		pos.add("cool");
		pos.add("class");
		pos.add("classy");
		pos.add("check");
		pos.add("delicious");
		pos.add("decent");
		pos.add("enjoy");
		pos.add("enjoyable");
		pos.add("enjoyed");
		pos.add("excellent");
		pos.add("enormous");
		pos.add("friendly");
		pos.add("fresh");
		pos.add("fantastic");
		pos.add("fan-freakin-tastic");
		pos.add("flavorable");
		pos.add("favorable");
		pos.add("favorite");
		pos.add("good");
		pos.add("gem");
		pos.add("great");
		pos.add("gigantic");
		pos.add("hit");
		pos.add("high");
		pos.add("happy");
		pos.add("huge");
		pos.add("juicy");
		pos.add("jumbo");
		pos.add("lot");
		pos.add("luxurious");
		pos.add("love");
		pos.add("like");
		pos.add("liked");
		pos.add("loved");
		pos.add("loads");
		pos.add("massive");
		pos.add("non-corporate");
		pos.add("nice");
		pos.add("pretty");
		pos.add("perfect");
		pos.add("perfection");
		pos.add("positive");
		pos.add("quality");
		pos.add("quantity");
		pos.add("reasonable");
		pos.add("superb");
		pos.add("super");
		pos.add("special");
		pos.add("speciality");
		pos.add("soft");
		pos.add("tasty");
		pos.add("taste");
		pos.add("traditional");
		pos.add("thick");
		pos.add("tender");
		pos.add("try");
		pos.add("vestige");
		pos.add("very");
		pos.add("wonderful");
		pos.add("worth");
		pos.add("worthy");

		// adding negative words
		neg.add("aint");
		neg.add("bad");
		neg.add("blight");
		neg.add("blighted");
		neg.add("burnt");
		neg.add("compromise");
		neg.add("can't");
		neg.add("crap");
		neg.add("don't");
		neg.add("dismal");
		neg.add("expensive");
		neg.add("hate");
		neg.add("isn't");
		neg.add("mistake");
		neg.add("miss");
		neg.add("never");
		neg.add("not");
		neg.add("no");
		neg.add("negative");
		neg.add("sad");
		neg.add("unflavored");
		neg.add("unfavorable");
		neg.add("worse");
		neg.add("worst");
	}
}
