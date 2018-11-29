package top.newforesee.bean

import scala.beans.BeanProperty

/**
  * 封装了某个品类总的点击、下单和支付数量的实体类
  * creat by newforesee 2018/11/28
  */
class CategoryBean extends Ordered[CategoryBean] with Serializable{
  /**
    * 点击的品类id,@BeanProperty注解可以自动为属性生成对应的getter/setter访问器
    */
  @BeanProperty var click_category_id: Long = 0
  /**
    * 当前品类总的点击次数
    */
  @BeanProperty var total_click_cnt:Long = 0
  /**
    * 当前品类总的下单次数
    */
  @BeanProperty var total_order_cnt:Long = 0
  /**
    * 当前品类总的支付次数
    */
  @BeanProperty var total_pay_cnt:Long = 0

  /**
    * 辅助构造器
    * @param click_category_id click_category_id
    * @param total_click_cnt total_click_cnt
    * @param total_order_cnt total_order_cnt
    * @param total_pay_cnt total_pay_cnt
    */
  def this(click_category_id: Long,total_click_cnt:Long,total_order_cnt:Long ,total_pay_cnt:Long)={
    this()
    this.click_category_id = click_category_id
    this.total_click_cnt = total_click_cnt
    this.total_order_cnt = total_order_cnt
    this.total_pay_cnt = total_pay_cnt

  }

  /**
    * 定制比较规则
    * @param that 与之比较的CategoryBean
    * @return
    */
  override def compare(that: CategoryBean): Int = {
    var ret: Long = that.total_click_cnt-this.total_click_cnt
    if (ret==0) {
      ret=that.total_order_cnt-this.total_order_cnt
      if (ret==0) {
        ret=that.total_pay_cnt-this.total_pay_cnt
      }
    }
    ret.toInt
  }
}
