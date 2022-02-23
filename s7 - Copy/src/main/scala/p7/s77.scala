package p7


import org.apache.spark.sql.SparkSession
import p7.Config._

object s77 {
  def main(args: Array[String])
  {
   println("oo999ll") 
   
   s.conf.getAll.foreach(println)
   import s.implicits._
   val df = s.read.format("csv").option("header","true").load(args(0))
   
   df.show()
   
   df.write.mode("Overwrite").save("C:\\Datas\\Bigdata\\spark_data_files\\tgt\\parq\\1")
   
   //df.write.partitionBy("gender","City").mode("Overwrite").save("C:\\Datas\\Bigdata\\spark_data_files\\tgt\\parq\\p1")
   
   df.select("gender","City").filter($"City" ==="Agassiz").write.partitionBy("gender").mode("Overwrite").save("C:\\Datas\\Bigdata\\spark_data_files\\tgt\\parq\\p2")
   
   val tdf=s.read.load("C:\\Datas\\Bigdata\\spark_data_files\\tgt\\parq\\1")
  println("Count="+tdf.count)
  
  tdf.printSchema
  }
}