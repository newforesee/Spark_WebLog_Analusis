package top.newforesee.jobs.goods

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession
import top.newforesee.bean.{Task, TaskParam}
import top.newforesee.dao.common.ITaskDao
import top.newforesee.dao.common.impl.TaskDaoImpl
import top.newforesee.mock.MockData
import top.newforesee.utils.ResourcesUtils

/**
  * 热门商品离线统计
  * creat by newforesee 2018/11/30
  */
object HotGoodsAnalysisJob {


  def main(args: Array[String]): Unit = {
    //前提准备
    val spark: SparkSession = prepareOperate(args)
    //步骤：
    //①Spark作业接收taskid，查询对应的MySQL中的task，获取用户指定的筛选参数；
    filterSessionByCondition(spark, args)

  }

  def filterSessionByCondition(spark: SparkSession, args: Array[String]): Unit = {
    //①准备一个字符串构建器的实例StringBuffer，用于存储sql
    val builder = new StringBuilder
    builder.append("select i.city,u.click_product_id " +
      "from click_product_id u,user_info i " +
      "where u.user_id=i.user_id and u.click_product_id is not null"
    )
    //②根据从mysql中task表中的字段task_param查询到的值，进行sql语句的拼接
    val taskId: Int = args(0).toInt
    val taskDao: ITaskDao = new TaskDaoImpl
    val task: Task = taskDao.findTaskById(taskId)
    // task_param={"ages":[0,100],"genders":["男","女"],"professionals":["教师", "工人", "记者", "演员", "厨师", "医生", "护士", "司机", "军人", "律师"],"cities":["南京", "无锡", "徐州", "常州", "苏州", "南通", "连云港", "淮安", "盐城", "扬州"]})
    val taskParamJsonStr: String = task.getTask_param
    //使用FastJson将json对象格式的数据封装到实体类TaskParam
    val taskParam: TaskParam = JSON.parseObject[TaskParam](taskParamJsonStr,classOf[TaskParam])
    //获得参数
    val start_time: String = taskParam.getStart_time
    val end_time: String = taskParam.getEnd_time

    //start_time
    if (start_time != null) {

    }


  }


  /**
    * 准备操作
    *
    * @param args
    * @return
    */
  private def prepareOperate(args: Array[String]): SparkSession = {
    //0、拦截非法的操作
    if (args == null || args.length != 1) {
      println("参数录入错误或是没有准备参数！请使用：spark-submit 主类  jar taskId")
      System.exit(-1)
    }

    //1、SparkSession的实例(注意：若分析的是hive表，需要启用对hive的支持，Builder的实例.enableHiveSupport())
    val builder: SparkSession.Builder = SparkSession.builder().appName(HotGoodsAnalysisJob.getClass.getSimpleName)

    //若是本地集群模式，需要单独设置
    if (ResourcesUtils.dMode.toString.toLowerCase().equals("local")) {
      builder.master("local[*]")
    }

    val spark: SparkSession = builder.getOrCreate()


    //2、将模拟的数据装载进内存（hive表中的数据）
    MockData.mock(spark.sparkContext, spark.sqlContext)

    //3、设置日志的显示级别
    spark.sparkContext.setLogLevel("WARN")

    //模拟数据测试：
    //spark.sql("select * from user_visit_action").show(1000)

    //4、返回SparkSession的实例
    spark
  }
}
