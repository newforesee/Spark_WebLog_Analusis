package top.newforesee.jobs.ad

import java.util
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import top.newforesee.bean.ad._
import top.newforesee.dao.ad.impl._
import top.newforesee.dao.ad._
import top.newforesee.utils.{DateUtils, ResourcesUtils, StringUtils}

/**
  * creat by newforesee 2018/11/30
  */
object AdFlowRealTimeCalJob {

  def main(args: Array[String]): Unit = {
    //前提
    //1.获得StreamingContext的实例
    val conf: SparkConf = new SparkConf().setAppName("AdFlowRealTimeCalJob")
    if (ResourcesUtils.dMode.toString.toLowerCase.equals("local")) {
      conf.setMaster("local[*]")
    }
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))
    sc.setLogLevel("WARN")


    //2.从消息队列中获取消息
    val dsFromKafka: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map("metadata.broker.list" -> "master:9092,slave1:9092,slave2:9092"), Set("ad_real_time_log"))
    //dsFromKafka.print()
    //3.设置checkpoint
    ssc.checkpoint("ck")
    //计算:
    //1.实时计算各batch中的每天各用户对各广告的点击次数
    val perDayDS: DStream[(String, Long)] = calPerDayUserClickAdCnt(dsFromKafka)

    //2.使用filter过滤点击广告超过一百次的用户
    filterBlackListToDB(perDayDS)

    //3.使用transform操作,对每个batch RDD进行处理,都动态加载MySQL中的黑名单生成RDD ,然后进行join后(左外连接查询),
    val whiteList: DStream[String] = getAllWhiteList(dsFromKafka)

    //4、 使用updateStateByKey操作，实时计算每天各省各城市各广告的点击量，并时候更新到MySQL (隐藏者一个前提：基于上一步的白名单)
    val dsClickCnt: DStream[(String, Long)] = calPerDayProvinceCityClickCnt(whiteList)

    //5、使用transform结合Spark SQL，统计每天各省份top3热门广告
    calPerDayProviceHotADTop3(dsClickCnt)
    //6、使用window操作，对最近1小时滑动窗口内的数据，计算出各广告各分钟的点击量，并更新到MySQL中
    calSlidewindow(whiteList)

    //后续操作
    //启动SparkStreaming
    ssc.start()

    //等待结束
    ssc.awaitTermination()
  }

  /**
    * 使用window操作，对最近1小时滑动窗口内的数据，计算出各广告各分钟的点击量，并更新到MySQL中
    *
    * @param whiteList 白名单数据
    */
  def calSlidewindow(whiteList: DStream[String]): Unit = {
    //reduceByKeyAndWindow((v1: Long, v2: Long) => v1 + v2, Minutes(60), Minutes(1))
    whiteList.map((item: String) => {
      val arr: Array[String] = item.toString.split("#")
      val time: String = DateUtils.formatTimeMinute(new Date(arr(0).trim.toLong))
      val adId: String = arr(4).trim
      (time + "#" + adId, 1L)
    }).reduceByKeyAndWindow((v1: Long, v2: Long) => v1 + v2, Seconds(30), Seconds(2))
      .foreachRDD((rdd: RDD[(String, Long)]) => {
        if (!rdd.isEmpty()) rdd.foreachPartition((itr: Iterator[(String, Long)]) => {
          val dao: IAdClickTrendDao = new AdClickTrendDaoImpl
          val beans: util.LinkedList[AdClickTrend] = new util.LinkedList[AdClickTrend]
          if (itr.nonEmpty) {
            itr.foreach((tuple: (String, Long)) => {
              val arr: Array[String] = tuple._1.split("#")
              val tmpDate: String = arr(0).trim
              val date: String = tmpDate.substring(0, tmpDate.length - 2)
              val minute: String = tmpDate.substring(tmpDate.length - 2)
              val ad_id: Int = arr(1).trim.toInt
              val click_count: Int = tuple._2.toInt
              val bean = new AdClickTrend(date, ad_id, minute, click_count)
              beans.add(bean)
            })
            dao.updateBatch(beans)
          }

        })
      })
  }

  /**
    *
    * @param dsClickCnt 实时计算每天各省各城市各广告的点击量
    */
  def calPerDayProviceHotADTop3(dsClickCnt: DStream[(String, Long)]): Unit = {
    var ssc: SQLContext = null
    //上一步DStream中每个元素为元组，形如：(每天#省#城市#广告id,总次数)，  如：（20181201#辽宁#朝阳#2，1）
    //reduceByKey ~>注意，此处必须使用reduceByKey，不能使用updateStateByKey,因为上一步已经统计迄今为止的总次数了！
    // 此步仅仅是基于上一步，将所有省份下所有城市的广告点击总次数聚合起来即可
    dsClickCnt.map((perEle: (String, Long)) => {
      val arr: Array[String] = perEle._1.split("#")
      val day: String = arr(0).trim
      val province: String = arr(1).trim
      val adId: String = arr(3).trim
      (StringUtils.appender("#", day, province, adId), perEle._2)
    }).reduceByKey((_: Long) + (_: Long))
      //注意：内部会将所有rdd的结果聚合起来
      .foreachRDD((rdd: RDD[(String, Long)]) => {
      //步骤：
      //①将RDD转换为DataFrame
      ssc=new SQLContext(rdd.sparkContext)
      val rowRDD: RDD[Row] = rdd.map((tuple: (String, Long)) => {
        val arr: Array[String] = tuple._1.split("#")
        val day: String = arr(0).trim
        val province: String = arr(1).trim
        val adId: String = arr(2).trim
        Row(day, province, adId, tuple._2)
      })
      rowRDD.checkpoint()
      val structType: StructType = {
        (new StructType)
          .add(StructField("day", StringType, nullable = false))
          .add(StructField("province", StringType, nullable = false))
          .add(StructField("adId", StringType, nullable = false))
          .add(StructField("click_count", LongType, nullable = false))
      }
      val df: DataFrame = ssc.createDataFrame(rowRDD,structType)
      //②将DataFrame映射为一张临时表
      df.createOrReplaceTempView("temp_ad_province")
      //③针对临时表求top3,将结果保存到db中
      ssc.sql("select *,row_number() over(partition by province order by click_count desc) rank from temp_ad_province having rank<=3")
        .rdd.foreachPartition((itr: Iterator[Row]) =>{
        if (itr.nonEmpty) {
          val dao: IAdProvinceTop3Dao = new AdProvinceTop3DaoImpl
          val beans: util.List[AdProvinceTop3] = new util.LinkedList[AdProvinceTop3]
          itr.foreach((row: Row) => {
            val date: String = row.getAs[String]("day")
            val province: String = row.getAs[String]("province")
            val ad_id: Int = row.getAs[String]("adId").toInt
            val click_count: Int = row.getAs[Long]("click_count").toInt

            val bean: AdProvinceTop3 = new AdProvinceTop3(date: String, province: String, ad_id: Int, click_count: Int)
            beans.add(bean)
          })

          dao.updateBatch(beans)

        }
      })

    })

  }

  /**
    *
    * @param whiteList 白名单
    * @return
    */
  def calPerDayProvinceCityClickCnt(whiteList: DStream[String]): DStream[(String, Long)] = {
    //步骤：
    //①实时计算每天各省各城市各广告的点击量
    //（1543635439749#河北#秦皇岛#36#2）
    val ds: DStream[(String, Long)] = whiteList.map((iter: String) => {
      val msg: String = iter
      val arr: Array[String] = msg.split("#")
      val day: String = DateUtils.formatDateKey(new Date(arr(0).toLong))
      (StringUtils.appender("#", day, arr(1), arr(4)), 1L)
    }).updateStateByKey((nowBatch: Seq[Long], historyAllBatch: Option[Long]) => {
      val nowSum: Long = nowBatch.sum
      val historySum: Long = historyAllBatch.getOrElse(0)
      Some(nowSum + historySum)
    })
    //②将统计的结果实时更新到MySQL
    ds.foreachRDD((rdd: RDD[(String, Long)]) => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition((itr: Iterator[(String, Long)]) => {
          val dao: IAdStatDao = new AdStatDaoImpl
          val beans: util.LinkedList[AdStat] = new util.LinkedList[AdStat]
          if (itr.nonEmpty) {
            itr.foreach((tuple: (String, Long)) => {
              val arr: Array[String] = tuple._1.split("#")
              val date: String = arr(0).trim
              val province: String = arr(1).trim
              val city: String = arr(2).trim
              val ad_id: Int = arr(3).trim.toInt
              val click_count: Int = tuple._2.toInt
              val bean = new AdStat(date, province, city, ad_id, click_count)
              beans.add(bean)
            })
            dao.updateBatch(beans)
          }

        })
      }
    })
    ds
  }


  /**
    *
    * @param dsFromKafka 从kafka获取的数据
    * @return
    */
  def getAllWhiteList(dsFromKafka: DStream[(String, String)]): DStream[String] = {
    // 过滤掉batch RDD中的黑名单用户的广告点击行为
    val whiteList: DStream[String] = dsFromKafka.transform((rdd: RDD[(String, String)]) => {
      //①动态加载mysql中的黑名单生成RDD
      val dao: IAdBlackListDao = new AdBlackListDaoImpl
      val allBlackList: util.List[AdBlackList] = dao.findAllAdBlackList()
      //注意:RDD实例中就能获取其所属的SparkContext的实例,不能从外面获取
      //      improt scala.collection.JavaConverters._
      import scala.collection.JavaConverters._
      val blackListRDD: RDD[(Int, String)] = rdd.sparkContext.parallelize(allBlackList.asScala).map((perBean: AdBlackList) => (perBean.getUser_id, ""))

      //将DStream中每个rdd中每个元素进行变形，(null,系统当前时间 省份 城市 用户id 广告id)~>变形为：(用户id，系统当前时间#省份#城市#用户id#广告id)
      val rddTmp: RDD[(Int, String)] = rdd.map((perEle: (String, String)) => {
        val arr: Array[String] = perEle._2.split("\\s+")
        val time: String = arr(0).trim
        val province: String = arr(1).trim
        val city: String = arr(2).trim
        val userId: Int = arr(3).toInt
        val adId: String = arr(4).trim
        (userId, StringUtils.appender("#", time, province, city, adId))

      })
      //黑名单RDD与DStream中当前的rdd进行左外连接查询, 设想其中一个结果：(34,(21312312321#湖北省#武汉市#34#2,None))
      val whiteAndBlackRDD: RDD[(Int, (String, Option[String]))] = rddTmp.leftOuterJoin(blackListRDD)
      //找出白名单
      val returnRDD: RDD[String] = whiteAndBlackRDD.filter((_: (Int, (String, Option[String])))._2._2.isEmpty).map((perEle: (Int, (String, Option[String]))) => {

        perEle._2._1
      })
      returnRDD
    })
    whiteList
  }

  /**
    * 使用filter过滤出每天对某个广告点击超过100次的黑名单用户，并写入MySQL中
    *
    * @param perDayDS 当天点击数据
    */
  private def filterBlackListToDB(perDayDS: DStream[(String, Long)]): Unit = {
    perDayDS.filter((perEle: (String, Long)) => perEle._2 > 100).foreachRDD((rdd: RDD[(String, Long)]) => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition((itr: Iterator[(String, Long)]) => {
          val dao: IAdBlackListDao = new AdBlackListDaoImpl
          val beans: util.List[AdBlackList] = new util.LinkedList

          if (itr.nonEmpty) {
            itr.foreach((tuple: (String, Long)) => {
              val userId: Int = tuple._1.split("#")(1).trim.toInt
              val bean = new AdBlackList(userId)
              beans.add(bean)
            })
            dao.updateBatch(beans)
          }
        })
      }
    })
  }

  /**
    *
    * @param dsFromKafka 从kafka获取的DStream数据
    * @return
    */
  def calPerDayUserClickAdCnt(dsFromKafka: DStream[(String, String)]): DStream[(String, Long)] = {
    //1.实时计算各batch中的每天各用户对各广告的点击次数
    val perDayDS: DStream[(String, Long)] = dsFromKafka.map((perMsg: (String, String)) => {
      val magValue: String = perMsg._2

      //将DStream中的每个元素转换成:(yyyy-MM-dd#userId#adId,1L)正则表达式： \s+，匹配：所有的空格：全角，半角，tab
      val arr: Array[String] = magValue.split("\\s+")
      val day: String = DateUtils.formatDate(new Date(arr(0).trim.toLong))
      val userId: String = arr(3).trim
      val adId: String = arr(4)
      (day + "#" + userId + "#" + adId, 1L)
    }).updateStateByKey[Long]((nowBatch: Seq[Long], history: Option[Long]) => {
      val nowSum: Long = nowBatch.sum
      val historySum: Long = history.getOrElse(0)
      Some(nowSum + historySum)
    })

    //使用高性能方式将每天各用户对广告的点击次数写入MySql中
    perDayDS.foreachRDD((rdd: RDD[(String, Long)]) => {
      if (!rdd.isEmpty()) rdd.foreachPartition((itr: Iterator[(String, Long)]) => {
        val dao: IADUserClickCountDao = new ADUserClickCountDaoImpl
        val beans: util.List[AdUserClickCount] = new util.LinkedList

        if (itr.nonEmpty) {
          itr.foreach((tuple: (String, Long)) => {
            val arr: Array[String] = tuple._1.split("#")
            val click_count: Int = tuple._2.toInt
            val date: String = arr(0).trim
            val user_id: Int = arr(1).trim.toInt
            val ad_id: Int = arr(2).trim.toInt
            val bean = new AdUserClickCount(date: String, user_id: Int, ad_id: Int, click_count: Int)
            beans.add(bean)
          })
          dao.updateBatch(beans)
        }
      })
    })
    perDayDS

  }

}

















