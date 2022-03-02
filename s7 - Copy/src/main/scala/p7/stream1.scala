package p7

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._

import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager


object stream1 {
  
  
  def main(args: Array[String]){
    
    println("starting")
    
    val s=SparkSession.builder().appName("Streaming").master("local[*]").getOrCreate()
    
    //s.conf.getAll.foreach(println)
    
    //s.sparkContext.setLogLevel("INFO")

  val logger: Logger =  LogManager.getLogger(this.getClass.getName);
    
     logger.info("Logger : Welcome to log4j")
     
    

    logger.debug("This is debug message");
        logger.info("This is info message");
        logger.warn("This is warn message");
        logger.fatal("This is fatal message");
        logger.error("This is error message");
       println("log level:::::"+ logger.getLevel())
    
    
    println("Stream:::")
    val initDF = s.readStream.format("rate").option("rowsPerSecond",1).load()

    println("Is Streaming? :: " + initDF.isStreaming)

    val resultDF = initDF.withColumn("result_________y", col("value") + lit(9))
    
    resultDF.writeStream.outputMode("append").option("truncate",false).format("console").start().awaitTermination()


  }
  
}