package p1

import org.apache.spark.sql.SparkSession

object Config {
  
  
  val s = SparkSession.builder().master("local").appName("APP-1").getOrCreate()
  

}