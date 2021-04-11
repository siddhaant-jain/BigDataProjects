package codeFiles

import java.util.Scanner
import java.io.{File, FileWriter}
import scala.reflect.io.Directory

object CreateNewDatasets {
  def deleteExistingDirectory(directoryPaths: String*): Boolean = {   
    for(directoryPath <- directoryPaths) {
      val dirFile = new File(directoryPath)
      if(!dirFile.exists())
        return true
      val dir = new Directory(dirFile)
      if(!dir.deleteRecursively())
        return false
    }
    true
  }
  def deleteExistingFiles(filePaths: String*) = {
    for(filePath <- filePaths) {
     val fileToDelete = new File(filePath)
    if(fileToDelete.exists())
      fileToDelete.delete() 
    }
    true
  }
  def createNewFiles(cleanedFilePath: String, reviewFilePath: String): Boolean = {
    val zomato_cleaned = new File(cleanedFilePath)
    val zomato_review = new File(reviewFilePath)
    
    if(zomato_cleaned.exists())
      zomato_cleaned.delete()
      
    if(zomato_review.exists())
      zomato_review.delete()
      
    if(!zomato_cleaned.createNewFile() || !zomato_review.createNewFile()){
      println("file could not be created")
      false
    }
    true
  }
  
  def createReviewsDataset(beforeReviewCols: String, review: String): String = {
    val url = beforeReviewCols.split(",")(0)
    if(review!=null)
      url+","+(review.replace(",", "#"))
    else
      url+","+review
  }
  
  def createDataset(originalFilePath: String, cleanedFilePath: String, reviewFilePath: String): Boolean = {
    
    val sc = new Scanner(new File(originalFilePath))
    sc.useDelimiter(",")
    
    if(!createNewFiles(cleanedFilePath, reviewFilePath))
      return false
    
    val cleanFileWriter = new FileWriter(cleanedFilePath)
    val reviewFileWriter = new FileWriter(reviewFilePath)
    
    val headerForReviewFile = "url,review"
    reviewFileWriter.write(headerForReviewFile+"\n")
    
    var prevRow: String = ""
    
    while(sc.hasNextLine()){
      val currRowString: String = sc.nextLine().replace("\\n", "|")
      
      //start of a new record
      if(currRowString.startsWith("https://www.zomato.com/")){
        val splitReview = prevRow.split("\\[\\(")
        //add all columns before review column
        prevRow = splitReview(0)
        var reviewToAdd = ""
        
        if(splitReview.size==2){
          val colsAfterReview = splitReview(1).split("\\)\\]")
          val reviews = splitReview(1).split("\\)\\]")(0)
          
          prevRow+=colsAfterReview(1)
          reviewToAdd+=createReviewsDataset(prevRow, reviews)
        }
        else{
          if(!prevRow.startsWith(" url"))
            reviewToAdd+=createReviewsDataset(prevRow, null)
        }
        
        //add previousRowToFile
        cleanFileWriter.write(prevRow + "\n")
        reviewFileWriter.write(reviewToAdd + "\n")
        
        //make this as previous row
        prevRow = currRowString
      }
      else{
        //continuation of last record
        prevRow += " " + currRowString
      }
      
    }
    
    cleanFileWriter.close()
    reviewFileWriter.close()
    return true
//    println("both files written successfully")
    
  }
}