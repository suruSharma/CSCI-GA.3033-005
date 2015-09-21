PROJECT NAME: Check-it Before Check-in 
SUBJECT: Real-time and Big Data, Spring 2015
MEMBERS	: Amartya, Nikita (nn899)
	  Desai, Rahul (rud206)
	  Sharma, Suruchi (sss665)

AIM: The aim of our project is to predict the possible rating of a restaurant by that a user who has not reviewed a restaurant would give it.

OVERVIEW: We have achieved this rating by selecting a set of users who have already reviewed the restaurant in questions and then computing the similarity measure using 
mathematical formulae, as described in the paper drop. Final rating is then calculated using weighted average which is on a scale of 0 to 100.

DEVELOPMENT: First stage in our development was getting the data from Yelp academic dataset challenge and convert it from JSON to a more readable format.
We have used three datasets from Yelp:
-> yelp_academic_dataset_business.json
-> yelp_academic_dataset_user.json
-> yelp_academic_dataset_review.json
These files were in json format, so we converted these to an easily readable format delimited by '|'. There were fields in the dataset which had comma so we chose '|' 
as the delimiter.
-> json_to_psv_converter.py : This python script does JSON to psv(Pipe Separated Values) conversion.
Next we needed to extract the fields that we needed from these datasets. For this we used MapReduce with Java and the corresponding code is in com.cbc package.
There are 3 set of MapReduce code one each for user, businesses and review dataset. We have used only businesses which are restaurants.
All the cleaned data was loaded to Hive so that we could run our analytic on it.
-> HiveQueries.txt : This file has the corresponding queries that we used on the cleaned data.
To find the rating(weight) for the textual reviews we have used 'Bag-of-Words' NLP technique. The corresponding UDF is in package com.reviewing.hive.udf. 
This gave us the review weights for each review.
-> AlalyticsQuery.txt: We have written a set of queries in Hive to get the final predicted values and these queries are in AnalyticsQuery.txt.			  
We got a list of top users and have run our analytic for a few set of users and businesses.
This file also has numeric results of our analytics on a small set of users.
-> Querying.java : This class takes as input the output of the hive query that fetches a list of users, restaurants and the review weight to evaluate the similarity between each user and the user in question to predict the final recommendation result.