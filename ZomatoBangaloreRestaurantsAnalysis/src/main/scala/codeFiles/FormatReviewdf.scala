package codeFiles

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.FloatType
import scala.reflect.api.materializeTypeTag

object FormatReviewdf {
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
    val avgRatingUf = udf[Option[Float], Seq[Float]]((x: Seq[Float]) => if(x==null || x.size<30) None else Some(x.sum/x.size))
    val minRatingUf = udf[Option[Float], Seq[Float]]((x: Seq[Float]) => if(x==null || x.size<30) None else Some(x(x.size-1)))
    val maxRatingUf = udf[Option[Float], Seq[Float]]((x: Seq[Float]) => if(x==null || x.size<30) None else Some(x(0)))
    
    df_rating
    .join(df_active, df_rating("url")===df_active("url") ,"inner")
    .withColumn("avg_rating", avgRatingUf(col("rating array")))
    .withColumn("min_rating", minRatingUf(col("rating array")))
    .withColumn("max_rating", maxRatingUf(col("rating array")))
    .select("name", "rate","location", "avg_rating", "min_rating", "max_rating")
    .filter("avg_rating is not null")
    .orderBy(col("avg_rating").desc)
  }
}