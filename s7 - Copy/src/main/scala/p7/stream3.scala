package p7

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._


import org.apache.spark.sql.types._

object stream3 {
  
  
  def main(args: Array[String]){
    
	  println("Starting..Stream3")

	  val s=SparkSession.builder().appName("Streaming").master("local[*]").getOrCreate()


	  println("===="+this.getClass.getName)

	  println("Stream:::")
	  val df = s.readStream.format("rate").option("rowsPerSecond",1).load()
	 


	  df.printSchema
	  println("Is Streaming? :: " + df.isStreaming)


	  val resultDF = df.withColumn("rate", col("value") + lit(9))
	  
	  resultDF.createOrReplaceTempView("a")
	  
	  //s.sql("select count(*) from a").writeStream.outputMode("Update").format("console").start()

	  	//  s.sql("select * from a").writeStream.outputMode("Update").format("console").start

	  
//s.sql("select count(*) from a").writeStream.outputMode("Complete").format("console").start()

//s.sql("select min(value) from a").writeStream.outputMode("Update").format("console").start()


//s.sql("select * from a").writeStream.outputMode("Append").format("parquet").option("path","C:\\Datas\\Logs\\1").option("checkpointLocation","C:\\Datas\\Logs\\2").start().awaitTermination()

//s.sql("select * from a").writeStream.outputMode("Append").format("csv").option("path","C:\\Datas\\Logs\\1").option("checkpointLocation","C:\\Datas\\Logs\\2").start().awaitTermination()
	
	  s.sql("select count(*) from a").writeStream.outputMode("Complete").format("parquet").option("path","C:\\Datas\\Logs\\1").option("checkpointLocation","C:\\Datas\\Logs\\2").start().awaitTermination()
	  
	  //resultDF.writeStream.outputMode("Update").format("console").start().awaitTermination()

	  //resultDF.writeStream.outputMode("append").option("truncate",false).format("console").start().awaitTermination()


  }
  
}