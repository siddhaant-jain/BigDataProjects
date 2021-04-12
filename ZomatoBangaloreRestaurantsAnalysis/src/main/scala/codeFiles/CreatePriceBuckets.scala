package codeFiles

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, col, desc, dense_rank, asc}
import org.apache.spark.sql.expressions.Window
import scala.reflect.api.materializeTypeTag

object CreatePriceBuckets {
  
  def findBucket(cost: String): Option[String] = {
    if(cost==null)
      return None
    Option(cost).getOrElse(None)
    val costString = cost.replaceAll("[^0-9]", "")
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
      .drop("rank")
  }
}