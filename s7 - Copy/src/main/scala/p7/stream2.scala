package p7

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._

import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager



object stream2 {
  
  
  def main(args: Array[String]){
    
    println("starting....")
    
    val s=SparkSession.builder().appName("Streaming").master("local[*]").getOrCreate()
   
     val logger: Logger =  LogManager.getLogger(this.getClass.getName);
    println("log level:::::"+ logger.getLevel())

     logger.error("Logger : Welcome to log4j")

     

  }
  
}