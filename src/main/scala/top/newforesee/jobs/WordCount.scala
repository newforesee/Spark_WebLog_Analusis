package top.newforesee.jobs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SparkWC").master("local").getOrCreate()
    val lines: Dataset[String] = spark.read.textFile("D:\\LocalWorkspace\\Idea project\\Spark_WebLog_Analusis\\src\\main\\resources\\wc.txt")
    val words: RDD[String] = lines.rdd.flatMap((_: String).split(" "))
    val tuple: RDD[(String, Int)] = words.map((_: String,1))
    val reduced: RDD[(String, Int)] = tuple.reduceByKey((_: Int)+(_: Int))
    val sorted: RDD[(String, Int)] = reduced.sortBy((_: (String, Int))._2,ascending = false)
    sorted.foreach(println)
    val rowRDD: RDD[Row] = sorted.map((row: (String, Int)) => {
      Row(row._1, row._2)
    })
    val structType: StructType = StructType(Array(StructField("word",StringType),StructField("num",IntegerType)))
    val df: DataFrame = spark.createDataFrame(rowRDD,structType)
    df.createOrReplaceTempView("wc")
    df.show()



  }

}
