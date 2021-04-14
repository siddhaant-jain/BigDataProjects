## Version used in project:
	|JRE:   | 1.8   |(Java 8)
	|Scala: | 11.11 |(Stable)
	|Spark  | 2.4.7 |(Compatible with Scala 11)
	|Kafka: | 2.6.0 |
	
## About the project
 > he basic idea of analyzing the Zomato dataset is to get a fair idea about the factors affecting the establishment
of different types of restaurant at different places in Bengaluru, aggregate rating of each restaurant, Bengaluru
being one such city has more than 12,000 restaurants with restaurants serving dishes from all over the world.
With each day new restaurants opening the industry has’nt been saturated yet and the demand is increasing
day by day. Inspite of increasing demand it however has become difficult for new restaurants to compete with
established restaurants. Most of them serving the same food. Bengaluru being an IT capital of India. Most of
the people here are dependent mainly on the restaurant food as they don’t have time to cook for themselves.
With such an overwhelming demand of restaurants it has therefore become important to study the demography
of a location. What kind of a food is more popular in a locality. Do the entire locality loves vegetarian food.
If yes then is that locality populated by a particular sect of people for eg. Jain, Marwaris, Gujaratis who are
mostly vegetarian. These kind of analysis can be done using the data, by studying the factors such as
• Location of the restaurant
• Approx Price of food
• Theme based restaurant or not
• Which locality of that city serves that cuisines with maximum number of restaurants
• The needs of people who are striving to get the best cuisine of the neighborhood
• Is a particular neighborhood famous for its own kind of food.
	
## To run this project:
1. Dataset
	* Dowload the dataset from the link: [Zomato Dataset](https://www.kaggle.com/himanshupoddar/zomato-bangalore-restaurants)
	* Extract it and paste in resources folder
2. Configure Winutils
	* Dowload winutils from the link: [Winutils Dowload](https://github.com/steveloughran/winutils)
	* Extract winutils and update its path in **filePath.properties** file
3. Make changes to **createSparkSession** function in **Application.scala** file if you're running on cloud
4. To run kafka task, you should have zookeeper and server running

## Tasks performed in this project
* Convert the data to parquet
* Tasks
	1. clean up :
		* Remove all non-ascii characters from all columns : Name, Location etc
		* Remove restaurants with no ratings.
	2. From the dataset:
		1. Filter out records which have invalid restaurant links (use regex to take main part of the url) – which means that the restaurant is most probably closed now.
		2. Group by Address location for the closed restaurants and find out which area has the most restaurants getting closed.
		3. For active restaurants, group by restaurant type and location and find out the restaurants which have highest rating for each cuisine type.
		4. For the reviews list column, find the distribution of star rating, on the condition that there are -at-least 30 ratings for that restaurant.
		5. Group by location for individual cost buckets (for 2 people) : [<=300, 300-500, 500-800, >= 800] and take the 5 highest rated restaurants in each location and each cost bucket and save as parquet file.
		6. For active restaurants, publish the data into individual kafka topics based on location.
* Use funsuite or scalatest to test functionality in local with multiple test cases with sample data.




** resources folder contains snapshot of outputs **
