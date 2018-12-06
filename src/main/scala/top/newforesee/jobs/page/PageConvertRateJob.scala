package top.newforesee.jobs.page

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import top.newforesee.bean.page.PageSplitConvertRate
import top.newforesee.bean.{Task, TaskParam}
import top.newforesee.constants.Constants
import top.newforesee.dao.common.ITaskDao
import top.newforesee.dao.common.impl.TaskDaoImpl
import top.newforesee.dao.page.IPageSplitConvertRate
import top.newforesee.dao.page.impl.PageSplitConvertRateImpl
import top.newforesee.mock.MockData
import top.newforesee.utils.{NumberUtils, ResourcesUtils, StringUtils}

import scala.collection.mutable

/**
  * creat by newforesee 2018/11/29
  */
object PageConvertRateJob {




  def main(args: Array[String]): Unit = {
    //前提准备
    val spark: SparkSession = prepareOperate(args)

    //1.按条件筛选筛选session
    val tuple: (RDD[Row], util.List[Integer]) = filterSessionByCondition(spark, args)
    //2.求页面单挑转化率
    val page_flow_temp: String = calPageConvert(spark, args, tuple)
    //3.将结果保存到数据库
    saveResultToDB(args, page_flow_temp)


    //4.释放资源
    spark.stop()
  }

  /**
    * 将结果保存到db中
    * @param args 主函数参数
    * @param page_flow_temp  页面单挑转化率结果
    */
  def saveResultToDB(args: Array[String], page_flow_temp: String): Unit = {
    val bean: PageSplitConvertRate = new PageSplitConvertRate(args(0).toInt, page_flow_temp)
    val dao: IPageSplitConvertRate = new PageSplitConvertRateImpl
    dao.saveToDB(bean)
  }

  /**
    * 求页面单条转化率
    *
    * @param spark SparkSession
    * @param args  Array[String]
    * @param tuple (RDD[Row], util.List[Integer])
    * @return
    */
  def calPageConvert(spark: SparkSession, args: Array[String], tuple: (RDD[Row], util.List[Integer])): String = {
    val rdd: RDD[Row] = tuple._1
    val page_flow: util.List[Integer] = tuple._2


    //前提:
    //①准备一个容器，用来存储每个页面的点击次数
    val container: mutable.HashMap[Int, Int] = new mutable.HashMap[Int, Int]()

    //②依次从页面流中取出每一个页面的编号，从RDD中与每个元素（页面id）进行比对,计算吻合的总次数，即为：当前页面点击的总数

    for (i <- 0 until page_flow.size){
      val page_id: Integer = page_flow.get(i)
      val nowPageTotalCnt: Long = rdd.filter((row: Row) =>row.getAs[Int]("page_id")==page_id).count()
      //将当前页面访问的次数存入到容器中
      container.put(page_id,nowPageTotalCnt.toInt)
    }

    var page_flow_str: StringBuilder = new mutable.StringBuilder()
    //  val PAGE_FLOW: String = "1_2=0|2_3=0|3_4=0|4_5=0|5_6=0|6_7=0|7_8=0|8_9=0|9_10=0"
    for (i <- 0 until page_flow.size()-1 ){
      val before: Integer = page_flow.get(i)
      val after: Integer = page_flow.get(i+1)
      page_flow_str.append(before).append("_").append(after).append(Constants.COMMON_INIT)
    }
    page_flow_str = page_flow_str.deleteCharAt(page_flow_str.length-1)
    var page_flow_temp: String = page_flow_str.toString()
    for(i<-0 until page_flow.size()-1){
      val now_page_id: Integer = page_flow.get(i)
      val next_page_id: Integer = page_flow.get(i+1)
      val field: String = next_page_id+"_"+next_page_id
      val now_page_id_cnt: Int = container(now_page_id)
      val next_page_id_cnt: Int = container(next_page_id)
      var rate = 0.0
      if (next_page_id_cnt==0 || now_page_id_cnt==0) {
        rate=0
      }else{
        rate = next_page_id_cnt.toDouble/now_page_id_cnt
      }
     page_flow_temp = StringUtils.setFieldInConcatString(page_flow_temp,"\\|",field,rate.toString)

    }

    page_flow_temp
  }


  /**
    * 准备操作
    *
    * @param args args: Array[String]
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
    * @param spark SparkSession
    * @param args  args: Array[String]
    */
  def filterSessionByCondition(spark: SparkSession, args: Array[String]): (RDD[Row], util.List[Integer]) = {
    //①准备一个字符串构建器的实例StringBuffer，用于存储sql
    val buffer = new StringBuffer
    buffer.append("select u.page_id from  user_visit_action u,user_info i where u.user_id=i.user_id ")

    //②根据从mysql中task表中的字段task_param查询到的值，进行sql语句的拼接
    val taskId: Int = args(0).toInt
    val taskDao: ITaskDao = new TaskDaoImpl
    val task: Task = taskDao.findTaskById(taskId)

    // task_param={"ages":[0,100],"genders":["男","女"],"professionals":["教师", "工人", "记者", "演员", "厨师", "医生", "护士", "司机", "军人", "律师"],"cities":["南京", "无锡", "徐州", "常州", "苏州", "南通", "连云港", "淮安", "盐城", "扬州"]})
    val taskParamJsonStr: String = task.getTask_param

    //使用FastJson，将json对象格式的数据封装到实体类TaskParam中
    val taskParam: TaskParam = JSON.parseObject[TaskParam](taskParamJsonStr, classOf[TaskParam])

    //获得参数值
    val ages: util.List[Integer] = taskParam.getAges
    val genders: util.List[String] = taskParam.getGenders
    val professionals: util.List[String] = taskParam.getProfessionals
    val cities: util.List[String] = taskParam.getCities

    //新增的条件
    val start_time: String = taskParam.getStart_time
    val end_time: String = taskParam.getEnd_time

    //后续计算的条件
    val page_flow: util.List[Integer] = taskParam.getPage_flow

    //ages
    if (ages != null && ages.size() > 0) {
      val minAge: Integer = ages.get(0)
      val maxAge: Integer = ages.get(1)
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
