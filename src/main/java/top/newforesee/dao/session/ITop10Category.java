package top.newforesee.dao.session;

import top.newforesee.bean.session.Top10Category;

/**
 * 操作特定品类点击、下单和支付总数对应的实体类的数据访问层<br/>
 * creat by newforesee 2018/11/29
 */
public interface ITop10Category {
    /**
     * 将参数指定的实例保存到db中
     *
     * @param bean
     */
    void saveBeanToDB(Top10Category bean);
}
