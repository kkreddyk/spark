package p7


import org.apache.spark.sql.SparkSession
import p7.Config._
import org.apache.log4j.Logger
import org.apache.log4j.Level



object s77 {
  def main(args: Array[String])
  {
   println("oo999ll...... ")
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)
   
   s.conf.getAll.foreach(println)
   import s.implicits._
   val df = s.read.format("csv").option("header","true").load(args(0))
   
 df.createOrReplaceTempView("aa")
 
 //s.sql("select * from aa where city='Abbotsford'").show(500,false)
  
 s.sql("select city,count(*),max(cast(age as integer)) from aa group by city having count(*) > 50 order by city").show(5,false)
   
 s.sql("select city,count(*),max(cast(age as integer)) from aa group by city having count(*) < 50 order by city").explain
  
   
   //df.write.mode("Overwrite").save("C:\\Datas\\Bigdata\\spark_data_files\\tgt\\parq\\1")
   
   //df.write.partitionBy("gender","City").mode("Overwrite").save("C:\\Datas\\Bigdata\\spark_data_files\\tgt\\parq\\p1")
   
   //df.select("gender","City").filter($"City" ==="Agassiz").write.partitionBy("gender").mode("Overwrite").save("C:\\Datas\\Bigdata\\spark_data_files\\tgt\\parq\\p2")
   
   val tdf=s.read.load("C:\\Datas\\Bigdata\\spark_data_files\\tgt\\parq\\1")
  println("Count="+tdf.count)
  
  //tdf.printSchema
  }
}