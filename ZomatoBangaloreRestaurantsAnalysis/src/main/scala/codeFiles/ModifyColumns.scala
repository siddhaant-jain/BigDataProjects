package codeFiles

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, col, explode, dense_rank, desc}
import org.apache.spark.sql.types.{ArrayType, StringType, FloatType}
import org.apache.spark.sql.expressions.Window
import scala.reflect.api.materializeTypeTag

object ModifyColumns {
  
  def columnToList(current_column: String): Option[Array[String]] = {
    Option(current_column).getOrElse(return None)
    
    Some(current_column.split(","))
  }
  
  def rateFloat(current_rate: String): Float = {
    if(current_rate==null || !current_rate.contains("/"))
      return 0.0f
    return current_rate.split("/")(0).trim().toFloat
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
      .drop("rank")
  }
}