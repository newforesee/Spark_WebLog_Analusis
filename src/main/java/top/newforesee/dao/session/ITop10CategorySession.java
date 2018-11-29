package top.newforesee.dao.session;

import top.newforesee.bean.session.Top10CategorySession;

/**
 * 存储top10每个品类的点击top10的session的数据访问层接口<br/>
 * creat by newforesee 2018/11/29
 */
public interface ITop10CategorySession {
    /**
     * @param bean
     */
    void saveBeanToDB(Top10CategorySession bean);
}
