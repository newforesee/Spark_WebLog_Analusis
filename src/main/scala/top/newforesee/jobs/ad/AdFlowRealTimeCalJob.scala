package top.newforesee.jobs.ad

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import top.newforesee.dao.ad.IADUserClickCountDao
import top.newforesee.dao.ad.impl.ADUserClickCountDaoImpl
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
    perDayDS.foreachRDD(rdd=>{
      if (!rdd.isEmpty()) rdd.foreachPartition((itr: Iterator[(String, Long)]) =>{
        val dao: IADUserClickCountDao = new ADUserClickCountDaoImpl

      })
    })


   //后续操作
    //启动SparkStreaming
    ssc.start()

    //等待结束
    ssc.awaitTermination()
  }

}

















