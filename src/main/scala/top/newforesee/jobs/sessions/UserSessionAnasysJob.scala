package top.newforesee.jobs.sessions

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import top.newforesee.bean.session._
import top.newforesee.bean.{CategoryBean, Task, TaskParam}
import top.newforesee.constants.Constants
import top.newforesee.dao.common.ITaskDao
import top.newforesee.dao.common.impl.TaskDaoImpl
import top.newforesee.dao.session.{ISessionAggrStat, ISessionDetail, ITop10Category, ITop10CategorySession}
import top.newforesee.dao.session.impl._
import top.newforesee.mock.MockData
import top.newforesee.utils.{ResourcesUtils, StringUtils}

import scala.collection.mutable.ArrayBuffer

/**
  * creat by newforesee 2018/11/27
  */
object UserSessionAnasysJob {



  def main(args: Array[String]): Unit = {
    //前提：
    val spark: SparkSession = prepareOperate(args)
    //步骤：
    //    1、按条件筛选session
    filterSeesionByCondition(args, spark)
    //    2、统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、
    //    10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个
    //      范围内的session占比
    getStepLenAndTimeLenRate(spark, args)

    //    3、在符合条件的session中，按照时间比例随机抽取1000个session
    randomExtract1000Session(spark, args)
    //    4、在符合条件的session中，获取点击、下单和支付数量排名前10的品类
    val categoryIdContainer: ArrayBuffer[Long] = calClickOrderPayTop10(spark, args)
    //    5、对于排名前10的品类，分别获取其点击次数排名前10的session
    calTop10ClickCntSession(categoryIdContainer, spark, args)

  }

  /**
    *  对于排名前10的品类，分别获取其点击次数排名前10的session
    * @param categoryIdContainer
    * @param spark
    * @param args
    */
  def calTop10ClickCntSession(categoryIdContainer: ArrayBuffer[Long], spark: SparkSession, args: Array[String]): Unit = {
    val dao: ITop10CategorySession = new Top10CategorySessionImpl

    for (category_id <- categoryIdContainer) {
      spark.sql("select  session_id, count(*) cnt   from filter_after_action where click_category_id=" + category_id + " group by session_id")
        .createOrReplaceTempView("temp_click_session")

      spark.sql("select  * from temp_click_session order by cnt desc").take(10).foreach(row => {
        val session_id: String = row.getAs[String]("session_id")
        val click_count: Long = row.getAs[Long]("cnt")

        val bean = new Top10CategorySession(args(0).toInt, category_id.toInt, session_id, click_count.toInt)
        dao.saveBeanToDB(bean)
      })
    }


  }

  /**
    *
    * @param spark
    * @param args
    * @return
    */
  def calClickOrderPayTop10(spark: SparkSession, args: Array[String]): ArrayBuffer[Long] = {
    //前提
    //1,准备一个容器
    val container: ArrayBuffer[CategoryBean] = new ArrayBuffer[CategoryBean]
    //2,准备一个容器.存放点击,下单和支付数量排名前十的品类id,供下一步使用
    val categoryIdContainer: ArrayBuffer[Long] = new ArrayBuffer
    //①设计一个实体类CategoryBean , 需要实现Ordered 特质(类似于Java中的Comparable)
    //②求出所有品类总的点击次数
    val arr: Array[Row] = spark.sql("select " +
      "click_category_id, count(*) total_click_cnt from " +
      "filter_after_action where click_category_id is not null " +
      "group by click_category_id ").rdd.collect
    val rdd: RDD[Row] = spark.sql("select * from filter_after_action").rdd.cache()

    //③根据不同品类总的点击次数：
    for (row <- arr) {
      //点击品类的id
      val click_category_id: String = row.getAs[Long]("click_category_id").toString
      //总的点击次数
      val total_click_cnt: Long = row.getAs[Long]("total_click_cnt")
      //求该品类总的下单次数
      val total_order_cnt: Long = rdd.filter((row: Row) => click_category_id.equals(row.getAs[String]("order_category_ids"))).count()
      //求该品类总的支付次数
      val total_pay_cnt: Long = rdd.filter((row: Row) => click_category_id.equals(row.getAs[String]("pay_category_ids"))).count()
      //			  封装成~> CategoryBean的实例，且添加到容器中存储起来
      val bean: CategoryBean = new CategoryBean(click_category_id.toLong, total_click_cnt, total_order_cnt, total_pay_cnt)
      //将实例添加都容器
      container.append(bean)
    }

    //④将容器转换成RDD,使用spark中的二次排序算子求topN ~>注意点： RDD中每个元素的类型是对偶元组 （对偶元组：将元组中只有两个元素的元组。）
    val arrs: Array[(CategoryBean, String)] = spark.sparkContext.parallelize(container).map((perEle: CategoryBean) => (perEle, "")).sortByKey().take(10)
    val dao: ITop10Category = new Top10CategoryImpl
    for (tuple <- arrs) {
      val tmpBean: CategoryBean = tuple._1
      val bean: Top10Category = new Top10Category(args(0).toInt, tmpBean.getClick_category_id.toInt, tmpBean.getTotal_click_cnt.toInt, tmpBean.getTotal_order_cnt.toInt, tmpBean.getTotal_pay_cnt.toInt)
      dao.saveBeanToDB(bean)
      //向容器中存入top10品类的id
      categoryIdContainer.append(tmpBean.getClick_category_id)
    }
    //return
    categoryIdContainer
  }


  /**
    * 根据比率值从filter_after_action抽取session
    *
    * @param row
    * @param spark
    * @param totalSessionCnt
    */
  def extractSessionByRate(row: Row, spark: SparkSession, totalSessionCnt: Long): Unit = {

    val nowTimePeriod = row.getAs[String]("timePeriod")
    //      val rateValue: Double = row.getAs[Double]("rateValue")
    val rateValue: Double = row.getAs("rateValue").toString.toDouble

    val needTotalSessionCnt = if (totalSessionCnt > 1000) 1000 else totalSessionCnt
    val arr: Array[Row] = spark.sql("select session_id,action_time,search_keyword from filter_after_action where instr(action_time,'" + nowTimePeriod + "') >0").rdd.takeSample(true, (needTotalSessionCnt * rateValue).toInt)

    val rdd: RDD[Row] = spark.sparkContext.parallelize(arr)
    val structType: StructType = StructType(Seq(StructField("session_id", StringType, false), StructField("action_time", StringType, false), StructField("search_keyword", StringType, true)))

    spark.createDataFrame(rdd, structType).createOrReplaceTempView("temp_random")

  }

  /**
    * 将结果映射为一张临时表，聚合后保存到db中
    *
    * @param spark
    * @param bcTaskId
    * @param bcContainer
    */
  def aggrResultToDB(spark: SparkSession, bcTaskId: Broadcast[Int], bcContainer: Broadcast[ArrayBuffer[String]]): Unit = {
    val nowPeriodAllSessionRDD: RDD[Row] = spark.sql(" select  session_id,concat_ws(',', collect_set(distinct search_keyword)) search_keywords ,min(action_time) start_time,max(action_time) end_time from temp_random group by session_id").rdd

    //将结果映射为一张临时表集合后保存到DB
    nowPeriodAllSessionRDD.foreachPartition(itr => {
      if (!itr.isEmpty) {
        //将迭代器中的记录取出来，储存到集合中
        val beans: util.LinkedList[SessionRandomExtract] = new util.LinkedList[SessionRandomExtract]()
        itr.foreach(row => {
          val task_id: Int = bcTaskId.value
          val session_id: String = row.getAs[String]("session_id")
          val satrt_time: String = row.getAs[String]("start_time")
          val end_time: String = row.getAs[String]("end_time")
          val search_keywords: String = row.getAs[String]("search_keywords")
          val bean: SessionRandomExtract = new SessionRandomExtract(task_id, session_id, satrt_time, end_time, search_keywords)
          beans.add(bean)
          bcContainer.value.append(session_id)
        })
        //准备dao层的实例ISessionRandomExtract

        val dao: SessionRandomExtractImpl = new SessionRandomExtractImpl
        dao.saveBeansToDB(beans)
      }

    })

  }

  /**
    * 向存储随机抽取出来的session的明细表中存储数据
    *
    * @param spark
    * @param bcTaskId
    * @param bcContainer
    */
  def randomSessionToDetail(spark: SparkSession, bcTaskId: Broadcast[Int], bcContainer: Broadcast[ArrayBuffer[String]]): Unit = {
    //将容器映射为一张临时表，与filter_after_action进行内连接查询
    val rddContainer: RDD[Row] = spark.sparkContext.parallelize(bcContainer.value).map((perEle: String) => Row(perEle))
    val structTypeContainer: StructType = StructType(Seq(StructField("session_id", StringType, nullable = true)))
    spark.createDataFrame(rddContainer, structTypeContainer).createOrReplaceTempView("container_temp")
    val dao: ISessionDetail = new SessionDetailImpl

    spark.sql("select * from container_temp t,filter_after_action f where t.session_id=f.session_id").rdd.collect.foreach(row => {
      val task_id = bcTaskId.value
      val user_id = row.getAs[Long]("user_id").toInt
      val session_id = row.getAs[String]("session_id")
      val page_id = row.getAs[Long]("page_id").toInt
      val action_time = row.getAs[String]("action_time")
      val search_keyword = row.getAs[String]("search_keyword")
      val click_category_id = row.getAs[Long]("click_category_id").toInt
      val click_product_id = row.getAs[Long]("click_product_id").toInt
      val order_category_ids = row.getAs[String]("order_category_ids")
      val order_product_ids = row.getAs[String]("order_product_ids")
      val pay_category_ids = row.getAs[String]("pay_category_ids")
      val pay_product_ids = row.getAs[String]("pay_product_ids")

      val bean = new SessionDetail(task_id, user_id, session_id, page_id, action_time, search_keyword, click_category_id, click_product_id, order_category_ids, order_product_ids, pay_category_ids, pay_product_ids)
      dao.saveToDB(bean)
    }
    )
  }

  /**
    * 在符合条件的session中，按照时间比例随机抽取1000个session
    *
    * @param spark
    * @param args
    */
  def randomExtract1000Session(spark: SparkSession, args: Array[String]): Unit = {
    //    //前提：准备一个容器，用于存储session_id
    val container: ArrayBuffer[String] = new ArrayBuffer
    val bcContainer: Broadcast[ArrayBuffer[String]] = spark.sparkContext.broadcast[ArrayBuffer[String]](container)

    //①求出每个时间段内的session数占总session数的比例值（不去重的session数）
    val totalSessionCnt: Long = spark.sql("select count(*) totalSessionCnt  from filter_after_action").first.getAs[Long]("totalSessionCnt")

    val rdd: RDD[Row] = spark.sql("select  substring(action_time,1,13) timePeriod, count(*)/" + totalSessionCnt.toDouble + " rateValue  from filter_after_action group by substring(action_time,1,13)").rdd
    val bcTaskId: Broadcast[Int] = spark.sparkContext.broadcast[Int](args(0).toInt)
    //②根据比例值rdd，从指定的时段内随机抽取相应数量的session,并变形后保存到db中
    rdd.collect.foreach(row => {
      //循环分析rdd,每循环一次
      // 根据比率值从filter_after_action抽取session
      extractSessionByRate(row, spark, totalSessionCnt)

      // 将结果映射为一张临时表，聚合后保存到db中
      aggrResultToDB(spark, bcTaskId, bcContainer)

    })

    //③向存储随机抽取出来的session的明细表中存取数据
    //容器中存取的session_id与filter_after_action表进行内连接查询，查询处满足条件的记录保存到明细表中
    randomSessionToDetail(spark, bcTaskId, bcContainer)


  }

  /**
    *
    * 统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、
    * 10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个
    * 范围内的session占比
    *
    * @param spark
    * @param args
    */
  def getStepLenAndTimeLenRate(spark: SparkSession, args: Array[String]): Unit = {

    //①根据session_id进行分组，求出各个session的步长和时长
    //注册自定义函数
    spark.udf.register("getTimeLen", (endTime: String, startTime: String) => getTimeLen(endTime, startTime))
    val rdd: RDD[Row] = spark.sql("select count(*) stepLen,getTimeLen(max(action_time),min(action_time)) timeLen from filter_after_action group by session_id").rdd
    //准备一个自定义累加器的实例，并进行注册
    val acc: SessionAggrStatAccumulator = new SessionAggrStatAccumulator
    spark.sparkContext.register(acc)
    //②将结果转换成rdd，循环分析RDD
    rdd.collect.foreach((row: Row) => {
      //循环体：
      //session_count累加1
      acc.add(Constants.SESSION_COUNT)

      //将当前的补偿与各个步长进行对比，若吻合，当前的步长累加1
      calStepLenSessionCnt(row, acc)

      //将当前的时长与各个时长对比，若吻合，当前的时长累加1

      calTimeLenSessionCnt(row, acc)
    })
    //③将最终的结果保存到db中的session_aggr_stat表
    saveSessionAggrStatToDB(acc, args)

  }

  /**
    * 将最终的结果保存到db中的session_aggr_stat表
    *
    * @param acc
    * @param args
    */
  def saveSessionAggrStatToDB(acc: SessionAggrStatAccumulator, args: Array[String]): Unit = {

    //session_count=189|1s_3s=20|4s_6s...|60=90
    val finalResult: String = acc.value

    val session_count = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.SESSION_COUNT).toInt
    val period_1s_3s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_1s_3s).toDouble / session_count
    val period_4s_6s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_4s_6s).toDouble / session_count
    val period_7s_9s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_7s_9s).toDouble / session_count
    val period_10s_30s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_10s_30s).toDouble / session_count
    val period_30s_60s = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_30s_60s).toDouble / session_count
    val period_1m_3m = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_1m_3m).toDouble / session_count
    val period_3m_10m = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_3m_10m).toDouble / session_count
    val period_10m_30m = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_10m_30m).toDouble / session_count
    val period_30m = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.TIME_PERIOD_30m).toDouble / session_count
    val step_1_3 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_1_3).toDouble / session_count
    val step_4_6 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_4_6).toDouble / session_count
    val step_7_9 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_7_9).toDouble / session_count
    val step_10_30 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_10_30).toDouble / session_count
    val step_30_60 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_30_60).toDouble / session_count
    val step_60 = StringUtils.getFieldFromConcatString(finalResult, "\\|", Constants.STEP_PERIOD_60).toDouble / session_count
    val bean: SessionAggrStat = new SessionAggrStat(args(0).toInt, session_count, period_1s_3s, period_4s_6s, period_7s_9s, period_10s_30s, period_30s_60s, period_1m_3m, period_3m_10m, period_10m_30m, period_30m, step_1_3, step_4_6, step_7_9, step_10_30, step_30_60, step_60)

    //println(finalResult)
    val dao: ISessionAggrStat = new SessionAggrStatImpl
    dao.saveBeanToDB(bean)
  }

  /**
    * 求相应时长范围内的session数
    *
    * @param row
    * @param acc
    */
  def calTimeLenSessionCnt(row: Row, acc: SessionAggrStatAccumulator): Unit = {
    val nowTimeLen: Long = row.getAs[Long]("timeLen")
    val timeLenSeconds: Long = nowTimeLen / 1000
    val timeLenMinutes: Long = timeLenSeconds / 60


    if (timeLenSeconds >= 1 && timeLenSeconds <= 3) {
      acc.add(Constants.TIME_PERIOD_1s_3s)
    } else if (timeLenSeconds >= 4 && timeLenSeconds <= 6) {
      acc.add(Constants.TIME_PERIOD_4s_6s)
    } else if (timeLenSeconds >= 7 && timeLenSeconds <= 9) {
      acc.add(Constants.TIME_PERIOD_7s_9s)
    } else if (timeLenSeconds >= 10 && timeLenSeconds <= 30) {
      acc.add(Constants.TIME_PERIOD_10s_30s)
    } else if (timeLenSeconds > 30 && timeLenSeconds < 60) {
      acc.add(Constants.TIME_PERIOD_30s_60s)
    } else if (timeLenMinutes >= 1 && timeLenMinutes < 3) {
      acc.add(Constants.TIME_PERIOD_1m_3m)
    } else if (timeLenMinutes >= 3 && timeLenMinutes < 10) {
      acc.add(Constants.TIME_PERIOD_3m_10m)
    } else if (timeLenMinutes >= 10 && timeLenMinutes < 30) {
      acc.add(Constants.TIME_PERIOD_10m_30m)
    } else if (timeLenMinutes >= 30) {
      acc.add(Constants.TIME_PERIOD_30m)
    }
  }

  /**
    * 求session在相应步长中的个数
    *
    * @param row
    * @param acc
    */
  def calStepLenSessionCnt(row: Row, acc: SessionAggrStatAccumulator): Unit = {

    val nowStepLen: Long = row.getAs[Long]("stepLen")
    if (nowStepLen >= 1 && nowStepLen <= 3) {
      acc.add(Constants.STEP_PERIOD_1_3)
    } else if (nowStepLen >= 4 && nowStepLen <= 6) {
      acc.add(Constants.STEP_PERIOD_4_6)
    } else if (nowStepLen >= 7 && nowStepLen <= 9) {
      acc.add(Constants.STEP_PERIOD_7_9)
    } else if (nowStepLen >= 10 && nowStepLen <= 30) {
      acc.add(Constants.STEP_PERIOD_10_30)
    } else if (nowStepLen > 30 && nowStepLen <= 60) {
      acc.add(Constants.STEP_PERIOD_30_60)
    } else if (nowStepLen > 60) {
      acc.add(Constants.STEP_PERIOD_60)
    }
  }

  /**
    * 按照条件筛选session
    *
    * @param args
    * @param spark
    */
  private def filterSeesionByCondition(args: Array[String], spark: SparkSession): Unit = {
    //spark.sql("select * from user_visit_action").show(1000)
    //准备构建sql
    val buffer = new StringBuffer()
    //根据从mysql中task表中的字段task_param查询到值
    //    buffer.append("select u.session_id,u.action_time ,u.search_keyword,i.user_id,u.page_id,u.click_product_id, from user_visit_action u,user_info i where u.user_id=i.user_id ")
    buffer.append("select u.*,i.user_id from user_visit_action u,user_info i where u.user_id=i.user_id ")
    val taskId: Int = args(0).toInt
    val taskDao: ITaskDao = new TaskDaoImpl
    val task: Task = taskDao.findTaskById(taskId)
    val taskparamJsonStr: String = task.getTask_param
    //使用FastJson将json对象格式的数据封装到实体类TaskParam中
    val taskParam: TaskParam = JSON.parseObject(taskparamJsonStr, classOf[TaskParam])
    //获得参数值
    val ages: util.List[Integer] = taskParam.getAges
    val genders: util.List[String] = taskParam.getGenders
    val professionals: util.List[String] = taskParam.getProfessionals
    val cities: util.List[String] = taskParam.getCities

    //ages
    if (ages != null && ages.size() > 0) {
      val minAge: Integer = ages.get(0)
      val maxAge: Integer = ages.get(1)
      buffer.append("and i.age between ").append(minAge).append(" and ").append(maxAge)
    }

    //genders
    if (genders != null && genders.size() > 0) {

      buffer.append(" and i.sex in (").append(JSON.toJSONString(genders, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", "")).append(")")
    }

    //getProfessionals
    if (professionals != null && professionals.size() > 0) {
      buffer.append(" and i.professional in(").append(JSON.toJSONString(professionals, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", "")).append(")")
    }

    //cities
    if (cities != null && cities.size() > 0) {
      buffer.append(" and i.city in(").append(JSON.toJSONString(cities, SerializerFeature.UseSingleQuotes).replace("[", "").replace("]", "")).append(")")
    }
    //    println("sql语句："+buffer.toString)
    //    spark.sql(buffer.toString).show(2000)

    spark.sql(buffer.toString).createOrReplaceTempView("filter_after_action")
    spark.sqlContext.cacheTable("filter_after_action")

    //    spark.sql("select * from filter_after_action").show(1000)
  }

  /**
    * 准备操作
    *
    * @param args
    * @return
    */
  private def prepareOperate(args: Array[String]): SparkSession = {
    //拦截
    if (args == null || args.length != 1) {
      println("传入参数有误")
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
    MockData.mock(spark.sparkContext, spark.sqlContext)
    //设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  /**
    * 获得时长值
    *
    * @param endTime
    * @param startTime ，形如：2018-11-27 11:59:47
    * @return 返回毫秒值
    */
  def getTimeLen(endTime: String, startTime: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(endTime).getTime - sdf.parse(startTime).getTime
  }
}
