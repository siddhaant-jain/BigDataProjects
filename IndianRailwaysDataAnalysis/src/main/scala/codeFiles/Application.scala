package codeFiles

import org.apache.spark.sql.{SparkSession,DataFrame, Encoders}
import org.apache.spark.sql.functions.{from_json, explode, col}
import java.io.FileReader
import java.util.Properties
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.catalyst.ScalaReflection
import models.Train
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.WrappedArray
import models.TrainProperty

object Application {
  
  def getNewSparkSession() = SparkSession
      .builder()
      .appName("IndianRailways")
      .master("local")
      .getOrCreate()
      
  def getDataframeFromJson(path: String, session: SparkSession): DataFrame = {
    session.read.option("inferSchema", true).json(path)
  }
      
  def main(args: Array[String]){
    val filePathReader: FileReader = new FileReader("filePaths.properties")
    val pathProperty = new Properties()
    pathProperty.load(filePathReader)
    
    System.setProperty("hadoop.home.dir", pathProperty.getProperty("winutilsPath"));
    
    val spark = getNewSparkSession()
    
    val trainsDf_combined = getDataframeFromJson(pathProperty.getProperty("trainsFilePath"), spark)
    
//    trainsDf_combined.withColumn("features1",explode(col("features")).alias("features")).select("features.*").show()
//    trainsDf_combined.withColumn("features1",explode(col("features")).alias("features")).select("features.*").select("type").distinct().show()
    //since type column has just one value(feature) we will drop it
    
    
    val trainsDf = trainsDf_combined
                   .withColumn("features",explode(col("features")).alias("features"))
                   .select(col("features.geometry"), col("features.properties"))
                   
    trainsDf.select(col("geometry.*"), col("properties.*")).show()
    trainsDf.show()
    trainsDf.printSchema()
  }
}