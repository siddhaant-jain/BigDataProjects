package codeFiles

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{desc, udf}
import java.io.FileReader
import java.util.Properties
import org.apache.spark.sql.SaveMode

object Application {
  def getNewSparkSession() = SparkSession
      .builder()
      .appName("ZomatoBangalore")
      .master("local")
      .getOrCreate()
      
  def getDataframeFromCsv(path: String, session: SparkSession):DataFrame =
    session.read.format("csv")
           .option("header", "true")
           .option("inferSchema", "true")
           .load(path)
           
  def main(args: Array[String]){
    
    val filePathReader: FileReader = new FileReader("filePaths.properties")
    val pathProperty = new Properties()
    pathProperty.load(filePathReader)
    
    System.setProperty("hadoop.home.dir", pathProperty.getProperty("winutilsPath"));
    //create application
    val spark = getNewSparkSession()
    
    //path for datasets
    val path_of_original_dataset: String = pathProperty.getProperty("originalFilePath")
    
     val path_for_cleaned_dataset_temp: String = pathProperty.getProperty("tempCleanFilePath")
    val path_for_review_dataset_temp: String = pathProperty.getProperty("tempReviewFilePath")
    
    val path_for_cleaned_dataset: String = pathProperty.getProperty("cleanFilePath")
    val path_for_review_dataset: String = pathProperty.getProperty("reviewFilePath")
    
    val path_for_parquet_file: String = pathProperty.getProperty("costBucketPath")
    
//    CreateNewDatasets.deleteExistingFiles(path_for_cleaned_dataset_temp, path_for_review_dataset_temp)
//    CreateNewDatasets.deleteExistingDirectory(path_for_cleaned_dataset, path_for_parquet_file, path_for_review_dataset)    
    //import actual zomato dataset
    val original_df = getDataframeFromCsv(path_of_original_dataset, spark)
//    original_df.show()
    
//    //create new datasets
//    if(!CreateNewDatasets.createDataset(path_of_original_dataset, path_for_cleaned_dataset_temp, path_for_review_dataset_temp)){
//      println("files could not be created")
//      return
//    }
    
    //read dataset without review and save to parquet file
    val cleaned_df_temp = getDataframeFromCsv(path_for_cleaned_dataset_temp , spark)
                           .toDF(original_df.columns.map(coln => coln.trim()):_*)
                           .drop("reviews_list")  
   
    val df_reviews_temp = getDataframeFromCsv(path_for_review_dataset_temp , spark)
    
    //51716 records
//    val x = cleaned_df_temp.select("*").count()
//    val y = df_reviews_temp.select("*").count()
//    println("total records in cleaned dataset without review column: " + x + " total records in review dataset: " + y)
    
    cleaned_df_temp.withColumnRenamed("approx_cost(for two people)","approx_cost_for_two_people")
                   .withColumnRenamed("listed_in(type)","listed_in_type")
                   .withColumnRenamed("listed_in(city)","listed_in_city")
                   .write.mode(SaveMode.Overwrite).parquet(path_for_cleaned_dataset)
                   
    df_reviews_temp.write.mode(SaveMode.Overwrite).parquet(path_for_review_dataset)
    
    val cleaned_df = spark.read.parquet(path_for_cleaned_dataset)
    val df_reviews = spark.read.parquet(path_for_review_dataset)
    
//    cleaned_df.show()
//    df_reviews.show()
    
    cleaned_df.persist()
    
    cleaned_df.persist()
    //task 1.1 - remove non ascii characters from dataset 
    val df_ascii = RemoveNonAscii.removeNonAsciiFromDataFrame(cleaned_df)
//    df_ascii.show()
    
    //task 1.2 - Remove restaurant with no ratings
    val df_rating = df_ascii.na.drop(Array("rate"))
//    df_rating.show()
    
    cleaned_df.unpersist()
    df_rating.persist()
    
    //task 2.1 - filter closed restaurants
    val df_inactive = FilterClosedResturants.filterInactiveRestuarantsDataFrame(df_rating, false)
    val df_active = FilterClosedResturants.filterInactiveRestuarantsDataFrame(df_rating, true)
    df_rating.unpersist()
    df_active.persist()
//    df_active.show()
//    df_inactive.show()

    //task 2.2 - group inactive by location and find area with most closed restaurant (output: South Bangalore - 42)
    val areaWiseClosedRest = df_inactive.groupBy("location").count().orderBy(desc("count"))
//    println("Area with most closed restaurants: " + areaWiseClosedRest.first().toString())

    //task 2.3 - group active by location and restaurant type -- highest rated for each cusine type
    //change cuisine column and restaurant type column from string to list and extract rating/5 from rate column
    val df_updated = ModifyColumns.modify3Columns(df_active)
//    val df_exploded = ModifyColumns.getExplodedColumns(df_updated)
    val topRatedRest = ModifyColumns.getTopRatedCuisine(
                        ModifyColumns.getExplodedColumns(
                          df_updated
                          )
                        )
//    topRatedRest.show()
                        
     //task 2.4 - distribution of star rating, on the condition that there are at-least 30 ratings for that restaurant
//    val df_reviews = getDataframeFromCsv(path_for_review_dataset , spark)
//    val df_review_list = FormatReviewdf.splitReviewIntoList(df_reviews)
      val df_ratingStatsFromReview = FormatReviewdf.getStatsForRating(
                                      FormatReviewdf.splitReviewIntoList(df_reviews), df_active)
//    df_ratingStatsFromReview.show()
                                      
                                      
      //task 2.5 Group by location for individual cost buckets (for 2 people) : [<=300, 300-500, 500-800, >= 800] 
    //and take the 5 highest rated restaurants in each location and each cost bucket and save as parquet file. 
    val df_buckets = CreatePriceBuckets
                     .createBuckets(df_updated.select("name", "location", "approx_cost_for_two_people", "rate_out_of_5")
                     .withColumnRenamed("approx_cost_for_two_people", "cost"))
          
    val df_loc_costBucket = CreatePriceBuckets.locationBucketTop5(df_buckets)
//    df_loc_costBucket.write.mode(SaveMode.Overwrite).parquet(path_for_parquet_file)
//    df_loc_costBucket.show()
    
    
    //task 2.6 publish open restaurants to kafka
    PublishActiveRestuarants.activeRestProducer(df_active, spark)




    spark.close()
  }

}