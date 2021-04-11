package codeFiles

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{udf, col}
import scala.Range
import scala.reflect.api.materializeTypeTag

object RemoveNonAscii {
  //
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
}