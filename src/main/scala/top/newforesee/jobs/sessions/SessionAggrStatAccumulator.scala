package top.newforesee.jobs.sessions

import org.apache.spark.util.AccumulatorV2
import top.newforesee.constants.Constants
import top.newforesee.utils.StringUtils

/**
  * Description：自定义的累加器，用于记录session数据中各个统计维度出现的session的数量
  * 比如session时长：
  * 1s_3s=5
  * 4s_6s=8
  *
  * 比如session步长：
  * 1_3=5
  * 4_6=10
  *
  * 这种自定义的方式在工作过程中非常有用，可以避免写大量的tranformation操作
  * 在scala中自定义累加器，需要继承AccumulatorV2（spark旧版本需要继承AccumulatorParam）这样一个抽象类
  *
  * <br/>
  * Copyright (c) ， 2018， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author 徐文波
  * @version : 1.0
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, String] {
  //session_count=0|1s_3s=0|4s_6s...|60=0
  var result = Constants.AGGR_RESULT.toString // 初始值

  /**
    * 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序
    *
    * @return
    */
  override def isZero: Boolean = true


  /**
    * 拷贝一个新的AccumulatorV2
    *
    * @return
    */
  override def copy(): AccumulatorV2[String, String] = {
    val myAccumulator = new SessionAggrStatAccumulator()
    myAccumulator.result = this.result
    myAccumulator
  }

  /**
    * reset: 重置AccumulatorV2中的数据
    */
  override def reset(): Unit = result = Constants.AGGR_RESULT.toString

  /**
    * add: 操作数据累加方法实现，session_count=0|1s_3s=2|4s_6s...|60=0
    *
    * @param v  ,如：session_count
    */
  override def add(v: String): Unit = {
    val v1: String = result
    val v2: String = v
    if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
      var newResult = ""
      // 从v1中，提取v2对应的值，并累加
      //0
      val oldValue: String = StringUtils.getFieldFromConcatString(v1, "\\|", v2)
      if (oldValue != null) {
        val newValue: Int = oldValue.toInt + 1
        //session_count=1|1s_3s=0|4s_6s...|60=0
        newResult = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
      }
      result = newResult
    }
  }

  /**
    * 合并数据
    *
    * @param other
    */
  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case map: SessionAggrStatAccumulator =>
      result = other.value
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  /**
    * AccumulatorV2对外访问的数据结果
    *
    * @return
    */
  override def value: String = result
}

