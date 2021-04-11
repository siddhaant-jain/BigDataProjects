package codeFiles

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, col}

object FilterClosedResturants {
  
  def isRestuarantInactive(rest: String): Option[Boolean] = {
    Option(rest).getOrElse(return None)
    
    val mainUrl = rest.split("\\?")(0)
    val numOfPartitions = mainUrl.substring(8).split("/").size
    
    Some(numOfPartitions==2)
  }
  
  def filterInactiveRestuarantsDataFrame(df: DataFrame, needActive: Boolean) : DataFrame = {
    val filterInactiveRestuarantsUdf = udf[Option[Boolean], String](isRestuarantInactive)
    df.filter((col("url") rlike "http[s]://www.zomato.com/bangalore/.*") === needActive)
//    df.filter(filterInactiveRestuarantsUdf(col("url")) === !needActive)
  }
}