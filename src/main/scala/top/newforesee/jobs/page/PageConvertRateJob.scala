package top.newforesee.jobs.page

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import top.newforesee.bean.{Task, TaskParam}
import top.newforesee.dao.common.ITaskDao
import top.newforesee.dao.common.impl.TaskDaoImpl
import top.newforesee.mock.MockData
import top.newforesee.utils.ResourcesUtils

/**
  * creat by newforesee 2018/11/29
  */
object PageConvertRateJob {
  def main(args: Array[String]): Unit = {
    prepareOperate(args)

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
    val builder: SparkSession.Builder = SparkSession.builder().appName(PageConvertRateJob.getClass.getSimpleName)

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

  /**
    * 按条件筛选session
    *
    * @param spark
    * @param args
    */
  def filterSessionByCondition(spark: SparkSession, args: Array[String]): (RDD[Row], util.List[Integer]) = {
    //①准备一个字符串构建器的实例StringBuffer，用于存储sql
    val buffer = new StringBuffer
    buffer.append("select u.page_id from  user_visit_action u,user_info i where u.user_id=i.user_id ")

    //②根据从mysql中task表中的字段task_param查询到的值，进行sql语句的拼接
    val taskId = args(0).toInt
    val taskDao: ITaskDao = new TaskDaoImpl
    val task: Task = taskDao.findTaskById(taskId)

    // task_param={"ages":[0,100],"genders":["男","女"],"professionals":["教师", "工人", "记者", "演员", "厨师", "医生", "护士", "司机", "军人", "律师"],"cities":["南京", "无锡", "徐州", "常州", "苏州", "南通", "连云港", "淮安", "盐城", "扬州"]})
    val taskParamJsonStr = task.getTask_param();

    //使用FastJson，将json对象格式的数据封装到实体类TaskParam中
    val taskParam: TaskParam = JSON.parseObject[TaskParam](taskParamJsonStr, classOf[TaskParam])

    //获得参数值
    val ages = taskParam.getAges
    val genders = taskParam.getGenders
    val professionals = taskParam.getProfessionals
    val cities = taskParam.getCities

    //新增的条件
    val start_time = taskParam.getStart_time
    val end_time = taskParam.getEnd_time

    //后续计算的条件
    val page_flow = taskParam.getPage_flow

    //ages
    if (ages != null && ages.size() > 0) {
      val minAge = ages.get(0)
      val maxAge = ages.get(1)
      buffer.append(" and i.age between ").append(minAge + "").append(" and ").append(maxAge + "")
    }

    //genders
    if (genders != null && genders.size() > 0) {
      //希望sql: ... and i.sex in('男','女')
      //JSON.toJSONString(genders, SerializerFeature.UseSingleQuotes)~> ['男','女']
      buffer.append(" and i.sex  in(").append(JSON.toJSONString(genders, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", "")).append(")")
    }

    //professionals
    if (professionals != null && professionals.size() > 0) {
      buffer.append(" and i.professional  in(").append(JSON.toJSONString(professionals, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", "")).append(")")
    }

    //cities
    if (cities != null && cities.size() > 0) {
      buffer.append(" and i.city in(").append(JSON.toJSONString(cities, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", "")).append(")")
    }

    //start_time
    if (start_time != null) {
      buffer.append(" and u.action_time>='").append(start_time).append("'")
    }

    //end_time
    if (end_time != null) {
      buffer.append(" and u.action_time<='").append(end_time).append("'")
    }

    //③测试，将结果转化为RDD,与计算条件一并返回给调用点
    (spark.sql(buffer.toString).rdd, page_flow)
  }

}
