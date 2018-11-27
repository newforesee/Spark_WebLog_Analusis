package top.newforesee.jobs

import org.apache.spark.sql.SparkSession
import top.newforesee.mock.MockData
import top.newforesee.utils.ResourcesUtils

/**
  * creat by newforesee 2018/11/27
  */
object UserSessionAnasysJob {
  def main(args: Array[String]): Unit = {
    //拦截
    if(args==null || args.length!=1){
      println()
      System.exit(-1)
    }

    //初始化SparkSession实例(如果分析的是hive表需要启用hive支持.enableHiveSupport()）
    val builder: SparkSession.Builder = SparkSession.builder().appName(UserSessionAnasysJob.getClass.getSimpleName)

    //若是本地集群模式，需单独设置
    if (ResourcesUtils.dMode.toString.toLowerCase.equals("local")) {
      builder.master("local[*]")
    }
    val spark: SparkSession = builder.getOrCreate()

    //将模拟数据装进内存
    MockData.mock(spark.sparkContext,spark.sqlContext)
//设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    //spark.sql("select * from user_visit_action").show(1000)
    //准备构建sql
    val buffer = new StringBuffer()
    //根据从mysql中task表中的字段task_param查询到值
    buffer.append("select * from ")


  }
}















