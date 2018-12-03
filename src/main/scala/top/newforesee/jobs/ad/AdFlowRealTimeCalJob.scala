package top.newforesee.jobs.ad

import java.util
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import top.newforesee.bean.ad.{AdBlackList, AdUserClickCount}
import top.newforesee.dao.ad.impl.{ADUserClickCountDaoImpl, AdBlackListDaoImpl}
import top.newforesee.dao.ad.{IADUserClickCountDao, IAdBlackListDao}
import top.newforesee.utils.{DateUtils, ResourcesUtils}

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
    dsFromKafka.print()
    //3.设置checkpoint
    ssc.checkpoint("ck")
    //计算:
    //1.实时计算各batch中的每天各用户对各广告的点击次数
    val perDayDS: DStream[(String, Long)] = calPerDayUserClickAdCnt(dsFromKafka)



    //2.使用filter过滤点击广告超过一百次的用户
    filterBlackListToDB(perDayDS)



    //3.使用transform操作,对每个batch RDD进行处理,都动态加载MySQL中的黑名单生成RDD ,然后进行join后(左外连接查询),
    // 过滤掉batch RDD中的黑名单用户的广告点击行为
    dsFromKafka.transform((rdd: RDD[(String, String)]) => {
      //①动态加载mysql中的黑名单生成RDD
      val dao: IAdBlackListDao = new AdBlackListDaoImpl
      val allBlackList: util.List[AdBlackList] = dao.findAllAdBlackList()
      //注意:RDD实例中就能获取其所属的SparkContext的实例,不能从外面获取
      //      improt scala.collection.JavaConverters._
      import scala.collection.JavaConverters._
      val blackListRDD: RDD[(Int, String)] = rdd.sparkContext.parallelize(allBlackList.asScala).map((perBean: AdBlackList) => (perBean.getUser_id, ""))

      //将DStream中每个rdd中每个元素进行变形，(null,系统当前时间 省份 城市 用户id 广告id)~>变形为：(用户id，系统当前时间#省份#城市#用户id#广告id)
     rdd.map((perEle: (String, String)) => {
       val arr: Array[String] = perEle._2.split("\\s+")

      })

    })



    //4
//    dsFromKafka.map((tuple: (String, String)) =>{
//      val msg: String = tuple._2
//      val arr: Array[String] = msg.split("\\s+")
//      val day: String = DateUtils.formatDate(new Date(arr(0).trim.toLong))
//      (day+"#"+arr(1).trim+"#"+arr(2).trim+"#"+arr(4).trim,1L)
//    }).updateStateByKey((nowBatch:Seq[Long],historyAllBatch:Option[Long])=>{
//
//      Some(nowBatch.head+historyAllBatch.get)
//
//    })







    //后续操作
    //启动SparkStreaming
    ssc.start()

    //等待结束
    ssc.awaitTermination()
  }

  /**
    * 使用filter过滤出每天对某个广告点击超过100次的黑名单用户，并写入MySQL中
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

















