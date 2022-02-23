package p7

import org.apache.spark.sql.SparkSession

object Config {
  
  
  val s=SparkSession.builder.master("local[*]").appName("S77").getOrCreate()
 
  
  
}