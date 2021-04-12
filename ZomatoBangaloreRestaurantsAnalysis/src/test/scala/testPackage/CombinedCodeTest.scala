package mainPackageTest

import org.scalatest.FunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File
import java.nio.file._
import org.scalatest.BeforeAndAfterAll
import java.nio.file.attribute.BasicFileAttributes

import  allCodePackage.CombinedCode
import java.io.FileReader
import java.util.Properties


class CombinedCodeTest extends FunSuite with BeforeAndAfterAll {
  val filePathReader: FileReader = new FileReader("filePaths.properties")
  val pathProperty = new Properties()
  pathProperty.load(filePathReader)
  System.setProperty("hadoop.home.dir", pathProperty.getProperty("winutilsPath"));
  
  var sparkSession : SparkSession = _

  override protected def beforeAll(): Unit = {
    sparkSession = SparkSession
      .builder()
      .appName("capstone-project-test")
      .master("local")
      .getOrCreate()
//    sparkSession.sparkContext.setLogLevel("INFO")
  }

  override protected def afterAll(): Unit = sparkSession.close()
      
  test("Test getCleanedDataFromParquet"){
     val path: String = pathProperty.getProperty("cleanFilePath")//"D:/CapstoneProject/CapstoneFiles/zomato_cleaned.parquet"
     val cleaned_df = CombinedCode.getCleanedDataFromParquet(path, sparkSession)
     
     val count = cleaned_df.count()
     assert(count==51716)
  }
  
  test("Test getDataFrame from csv"){
    val path: String =pathProperty.getProperty("originalFilePath")// "D:/CapstoneProject/CapstoneFiles/zomato.csv"
    
    val df = CombinedCode.getDataframeFromCsv(path, sparkSession)
    
    val count = df.count()
    assert(count>=56251)
  }
  
  test("Test removeNonAscii"){    
    val asciiString = CombinedCode.removeNonAscii("""ab\x62\x75 cd""")
    assert(asciiString.getOrElse("") == """ab cd""")
    
    //    val asciiString = CombinedCode.removeNonAscii("A função")
//    assert(asciiString.getOrElse("") == "A funo")
  }
  
  test("Test getInActiveResturants"){
    val path: String = pathProperty.getProperty("cleanFilePath")//"D:/CapstoneProject/CapstoneFiles/zomato_cleaned.parquet"
    val df = CombinedCode.removeNonAsciiFromDataFrame(
              CombinedCode.getCleanedDataFromParquet(path, sparkSession))
            .na
            .drop(Array("rate"))
    val df_inactive = CombinedCode.filterInactiveRestuarantsDataFrame(df, false)
    
    assert(df_inactive.count()==412)
  }
  
  test("Test getActiveResturants"){
    val path: String = pathProperty.getProperty("cleanFilePath")//"D:/CapstoneProject/CapstoneFiles/zomato_cleaned.parquet"
    val df = CombinedCode.removeNonAsciiFromDataFrame(
              CombinedCode.getCleanedDataFromParquet(path, sparkSession))
            .na
            .drop(Array("rate"))
    val df_active = CombinedCode.filterInactiveRestuarantsDataFrame(df, true)
    assert(df_active.count()==43529)
  }
  
}