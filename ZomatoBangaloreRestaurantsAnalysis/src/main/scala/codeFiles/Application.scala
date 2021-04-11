package codeFiles

import org.apache.spark.sql.{SparkSession, DataFrame}
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
    
    CreateNewDatasets.deleteExistingFiles(path_for_cleaned_dataset_temp, path_for_review_dataset_temp)
    CreateNewDatasets.deleteExistingDirectory(path_for_cleaned_dataset, path_for_parquet_file, path_for_review_dataset)    
    //import actual zomato dataset
    val original_df = getDataframeFromCsv(path_of_original_dataset, spark)
//    original_df.show()
    
    //create new datasets
    if(!CreateNewDatasets.createDataset(path_of_original_dataset, path_for_cleaned_dataset_temp, path_for_review_dataset_temp)){
      println("files could not be created")
      return
    }
    
    //read dataset without review and save to parquet file
    val cleaned_df_temp = getDataframeFromCsv(path_for_cleaned_dataset_temp , spark)
                           .toDF(original_df.columns.map(coln => coln.trim()):_*)
                           .drop("reviews_list")  
   
    val df_reviews_temp = getDataframeFromCsv(path_for_review_dataset_temp , spark)
    
    //51716 records
//    val x = cleaned_df_temp.select("*").count()
//    val y = df_reviews_temp.select("*").count()
//    println(x + " " + y)
    
    cleaned_df_temp.withColumnRenamed("approx_cost(for two people)","approx_cost_for_two_people")
                   .withColumnRenamed("listed_in(type)","listed_in_type")
                   .withColumnRenamed("listed_in(city)","listed_in_city")
                   .write.mode(SaveMode.Overwrite).parquet(path_for_cleaned_dataset)
                   
    df_reviews_temp.write.mode(SaveMode.Overwrite).parquet(path_for_review_dataset)
    
    val cleaned_df = spark.read.parquet(path_for_cleaned_dataset)
    val df_reviews = spark.read.parquet(path_for_review_dataset)
    
    cleaned_df.persist()

    spark.close()
  }

}