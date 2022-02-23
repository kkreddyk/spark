import org.apache.spark.sql.SparkSession


object o1 {
  
  def main(args: Array[String])
  {
      println("sss")
      
      
    val s=SparkSession.builder.appName("Spark SQL basic example").config("spark.master", "local").getOrCreate()
    import s.implicits._
println(args(0))

s.conf.getAll.foreach(println)
    val e=s.read.format("csv").option("header","true").load(args(0))
    e.show
    
    
    val r=Range(1,args(1).toInt)
    
    val df=r.toDF
    
    df.count
    df.show(40)
 
    
    
    
    Thread.sleep(10000)
  }

}