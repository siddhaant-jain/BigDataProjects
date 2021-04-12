package allCodePackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, udf, col, explode, dense_rank, asc}
import org.apache.spark.sql.{DataFrame}
import scala.Range
import org.apache.spark.sql.types.{ArrayType, StringType, FloatType}
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.reflect.api.materializeTypeTag

case class Record (location: String, msg:String)

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}

object KafkaSink {
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      new KafkaProducer[String, String](config)
    }
    new KafkaSink(f)
  }
}

object CombinedCode {
  
  def activeRestProducer(active_restaurants: DataFrame, spark: SparkSession) = {
    
    val producerProp = new Properties()
		producerProp.put("bootstrap.servers", "localhost:9092")
		producerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		producerProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		
		val kafkaSink = spark.sparkContext.broadcast(KafkaSink(producerProp))

		val dfrdd = active_restaurants.rdd.map(row => {
		  val validTopic = "[^a-zA-Z0-9\\._\\-]"
		  val location: String = row.getAs("location").toString().replaceAll(validTopic,"")
		  val msg = row.getString(0)
		  Record(location, msg)
		})
		
		dfrdd.foreachPartition((partitions: Iterator[Record]) => {
//		  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerProp)
		  partitions.foreach((record: Record) => {
		    kafkaSink.value.send(record.location, record.msg)
//		    val dataToSend = new ProducerRecord[String, String](record.location,record.location,record.msg)
//		    producer.send(dataToSend)
		  })
		})
    
//    CAST( struct(" + col("location").getItem(0) + ") AS STRING) AS topic
    
//		active_restaurants.selectExpr("CAST( struct(" + col("location") + ") AS STRING) AS topic", "to_json(struct(*)) AS value")
//		  .write
//		  .format("kafka")
//		  .option("kafka.bootstrap.servers", "localhost:9092")
////		  .option("topic", "CAST(location AS STRING)")
//		  .save()
		
		System.out.println("All active restaurants published")
  }
  
  def findBucket(cost: String): Option[String] = {
    if(cost==null)
      return None
    Option(cost).getOrElse(None)
     val costString = ("""\d+""".r findAllIn cost).mkString("")//.toInt //cost.toInt
     var costInt = 0
     if(costString.matches("""\d+""")){
       costInt = costString.toInt
     }
    
     if(costInt>=800)
       Some(">=800")
     else if(costInt>=500 && costInt<800)
       Some("500-800")
     else if(costInt>300 && costInt<500)
       Some("300-500")
     else
       Some("<=300")
  }
  def createBuckets(df: DataFrame):DataFrame = {
    val findBucketUdf = udf[Option[String], String] (findBucket)
    df.withColumn("costBucket", findBucketUdf(col("cost")))
  }
  
  def locationBucketTop5(df: DataFrame): DataFrame = {
    val winLocBucket = Window.partitionBy("location").partitionBy("costBucket").orderBy(desc("rate_out_of_5"))//
    df.na.drop(Array("cost"))
      .select("location", "costBucket", "name", "cost", "rate_out_of_5")
      .distinct()
      .withColumn("rank", dense_rank().over(winLocBucket))
      .filter(col("rank")<=5)
      .sort(asc("location"),asc("costBucket"), desc("rate_out_of_5"))
      
//     df.na.drop(Array("cost")).groupBy(col("location"), col("costBucket"), col("name"), col("cost"))
//                           .agg(max(col("rate_out_of_5")).as("rate_out_of_5"))
//                           .withColumn("rank", row_number().over(Window.partitionBy(col("location"), col("costBucket")).orderBy(col("rate_out_of_5").desc)))
//                           .orderBy(col("location"), col("costBucket"), col("rate_out_of_5").desc)
//                           .filter(col("rank") < 6)
//                           .drop(col("rank"))
  }
  
  def reviewStringtoList(review: String): Array[Float] = { //, Array[String])
    if(review==null)
      return  null
    val reviewAndRatingList = review.split("\\)# \\(")
    val ratingList: ListBuffer[Float] = ListBuffer()
    
    reviewAndRatingList.foreach(f => {
      if(f!=null && f!=""){
        val rateRegex = """Rated \d.\d""".r //f.matches("Rated \\d\\.\\d")
        val rating = rateRegex.findFirstIn(f)
        val finalRate: String = rating.getOrElse("-1")
        if(finalRate!="-1")
          ratingList+=finalRate.split(" ")(1).toFloat
      }
    })
    ratingList.toArray.sorted.reverse
  }
  
  def splitReviewIntoList(df: DataFrame):DataFrame = {
    val reviewStringtoListudf = udf[Array[Float], String](reviewStringtoList)
    df.withColumn("rating array",reviewStringtoListudf(col("review")).cast(ArrayType(FloatType, true)))
  }
  
  def getStatsForRating(df_rating: DataFrame, df_active: DataFrame): DataFrame = {
    val avgRatingUf = udf[Float, Seq[Float]]((x: Seq[Float]) => if(x==null || x.size<30) Float.NaN else x.sum/x.size)
    val minRatingUf = udf[Float, Seq[Float]]((x: Seq[Float]) => if(x==null || x.size<30) Float.NaN else x(0))
    val maxRatingUf = udf[Float, Seq[Float]]((x: Seq[Float]) => if(x==null || x.size<30) Float.NaN else x(x.size-1))
    
    df_rating
    .join(df_active, df_rating("url")===df_active("url") ,"inner")
    .withColumn("avg rating", avgRatingUf(col("rating array")))
    .withColumn("min rating", minRatingUf(col("rating array")))
    .withColumn("max rating", maxRatingUf(col("rating array")))
    .select("name", "rate","location", "avg rating", "min rating", "max rating")
    .orderBy("avg rating")
  }
  
  def columnToList(current_column: String): Option[Array[String]] = {
    Option(current_column).getOrElse(return None)
    
    Some(current_column.split(","))
  }
  
  def rateFloat(current_rate: String): Float = {
    if(current_rate==null || !current_rate.contains("/"))
      return 0.0f
    return current_rate.split("/")(0).trim().toFloat
//    return current_rate.substring(0, 3).toFloat
  }
  
  def modify3Columns(activeRestDf: DataFrame): DataFrame = {
    val columnStringToListUdf = udf[Option[Array[String]], String](columnToList)
    val rateFloatUdf = udf[Float, String](rateFloat)
    
    activeRestDf
    .withColumn("cusine_list", columnStringToListUdf(col("cuisines")).cast(ArrayType(StringType, true)))
    .withColumn("rest_type_list", columnStringToListUdf(col("rest_type")).cast(ArrayType(StringType, true)))
    .withColumn("rate_out_of_5", rateFloatUdf(col("rate")).cast(FloatType))
  }
  
  def getExplodedColumns(df: DataFrame): DataFrame = {
    df.select(col("url"), col("name"), col("location"), col("rest_type_list"), col("rate_out_of_5"), explode(col("cusine_list"))
      .alias("cuisine"))
      .select(col("*"),explode(col("rest_type_list"))
      .alias("restaurantType"))
      .drop("rest_type_list")
      .drop("cusine_list")
  }
  
  def getTopRatedCuisine(df: DataFrame): DataFrame = {
    val winCuisine = Window.partitionBy("location", "restaurantType", "cuisine").orderBy(desc("rate_out_of_5"))
    df.select("location","cuisine","restaurantType", "name", "rate_out_of_5")
      .withColumn("rank", dense_rank().over(winCuisine))
      .filter(col("rank")===1)
      .distinct()
  }
  
  def filterInactiveRestuarantsDataFrame(df: DataFrame, needInactive: Boolean) : DataFrame = {
//    val filterInactiveRestuarantsUdf = udf[Option[Boolean], String](isRestuarantInactive)
    df.filter((col("url") rlike "http[s]://www.zomato.com/bangalore/.*") === needInactive)
//    df.filter(filterInactiveRestuarantsUdf(col("url")) === needInactive)
  }
  
  def removeNonAscii(colValue: String): Option[String] = {
    Option(colValue).getOrElse(return None) 
     Some(colValue.filter(p => (Range(0,128) contains p)).replaceAll("\\\\x[0-9]{2}", ""))
  }
  //
  def removeNonAsciiFromDataFrame(df: DataFrame): DataFrame = {
    val removeNonAsciiUdf = udf[Option[String], String](removeNonAscii)
    df.select( df.columns.map
              (col1 => 
                removeNonAsciiUdf(col(col1)).alias(col1)
              ): _*
             )
  }
  
  def getCleanedDataFromParquet(path_for_cleaned_dataset: String, spark: SparkSession):DataFrame = {
    spark.read.parquet(path_for_cleaned_dataset)
  }
  
  def getDataframeFromCsv(path: String, session: SparkSession):DataFrame =
    session.read.format("csv")
           .option("header", "true")
           .option("inferSchema", "true")
           .load(path)
           
  def main(args: Array[String]){
    
    System.setProperty("hadoop.home.dir", "C:/Users/siddhant.jain/Downloads/winutils-master/hadoop-2.7.1/");
    
    //create application
    val spark = SparkSession
      .builder()
      .appName("CapstoneProject")
      .master("local")
      .getOrCreate()
    
    //path for datasets
    val path_of_original_dataset: String = args(0)//"D:/CapstoneProject/CapstoneFiles/zomato.csv"
    val path_for_cleaned_dataset_temp: String = args(1)//"D:/CapstoneProject/CapstoneFiles/zomato_cleaned_temp.csv"
    val path_for_review_dataset_temp: String = args(2)//"D:/CapstoneProject/CapstoneFiles/zomato_review_temp.csv"
    val path_for_cleaned_dataset: String = args(3)//"D:/CapstoneProject/CapstoneFiles/zomato_cleaned.parquet"
    val path_for_review_dataset: String = args(4)//"D:/CapstoneProject/CapstoneFiles/zomato_review.parquet"
    val path_for_parquet_file: String = args(5)//"D:/CapstoneProject/CapstoneFiles/zomato_cost_bucket.parquet"
    
    //import real dataset
//    val original_df = getDataframeFromCsv(path_of_original_dataset, spark)
//    original_df.show()
    
    //create new datasets
//    if(!CreateNewDatasets.createDataset(path_of_original_dataset, path_for_cleaned_dataset_temp, path_for_review_dataset_temp)){
//      println("files could not be created")
//      return
//    }
    
    //read dataset without review and save to parquet file
//    val cleaned_df_temp = getDataframeFromCsv(path_for_cleaned_dataset_temp , spark)
//                           .toDF(original_df.columns.map(coln => coln.trim()):_*)
//                           .drop("reviews_list")  
//   
//    val df_reviews_temp = getDataframeFromCsv(path_for_review_dataset_temp , spark)
    
    //51716 records
//    val x = cleaned_df_temp.select("*").count()
//    val y = df_reviews_temp.select("*").count()
//    println(x + " " + y)
    
//    cleaned_df_temp.withColumnRenamed("approx_cost(for two people)","approx_cost_for_two_people")
//                   .withColumnRenamed("listed_in(type)","listed_in_type")
//                   .withColumnRenamed("listed_in(city)","listed_in_city")
//                   .write.mode(SaveMode.Overwrite).parquet(path_for_cleaned_dataset)
                   
//    df_reviews_temp.write.mode(SaveMode.Overwrite).parquet(path_for_review_dataset)
    
    val cleaned_df = getCleanedDataFromParquet(path_for_cleaned_dataset, spark)
    val df_reviews = spark.read.parquet(path_for_review_dataset)
    
    cleaned_df.persist()
    //task 1.1 - remove non ascii characters from dataset 
    val df_ascii = removeNonAsciiFromDataFrame(cleaned_df)
    

    
    //task 1.2 - Remove restaurant with no ratings
    val df_rating = df_ascii.na.drop(Array("rate"))
    cleaned_df.unpersist()
    df_rating.persist()
    
    //task 2.1 - filter closed restaurants
    val df_inactive = filterInactiveRestuarantsDataFrame(df_rating, true)
    val df_active = filterInactiveRestuarantsDataFrame(df_rating, false)
    df_rating.unpersist()
    df_active.persist()
//    df_active.show()
    
    //task 2.2 - group inactive by location
    val areaWiseClosedRest = df_inactive.groupBy("location").count().orderBy(desc("count")).limit(1)
//    areaWiseClosedRest.show()
    
    //task 2.3 - group active by location and restaurant type -- highest rated for each cusine type
    //change cuisine column and restaurant type column from string to list and extract rating/5 from rate column
    val df_updated = modify3Columns(df_active)
    val df_exploded = getExplodedColumns(df_updated)
    val topRatedRest = getTopRatedCuisine(df_exploded)
//    topRatedRest.show()
        
    //task 2.4 - distribution of star rating, on the condition that there are at-least 30 ratings for that restaurant
//    val df_reviews = getDataframeFromCsv(path_for_review_dataset , spark)
    val df_review_list = splitReviewIntoList(df_reviews)
    val df_ratingStatsFromReview = getStatsForRating(df_review_list, df_active)
//    df_ratingStatsFromReview.show()
    
    //task 2.5 Group by location for individual cost buckets (for 2 people) : [<=300, 300-500, 500-800, >= 800] 
    //and take the 5 highest rated restaurants in each location and each cost bucket and save as parquet file. 
    val df_buckets = createBuckets(df_updated.select("name", "location", "approx_cost_for_two_people", "rate_out_of_5")
                     .withColumnRenamed("approx_cost_for_two_people", "cost"))
          
//    val df_loc_costBucket = 
//      CreatePriceBuckets.locationBucketTop5(df_buckets).show(50)
//    df_loc_costBucket.write.mode(SaveMode.Overwrite).parquet(path_for_parquet_file)
    
    //task 2.6 publish open restaurants to kafka
    activeRestProducer(df_active, spark)
    
    spark.close()
  }
}