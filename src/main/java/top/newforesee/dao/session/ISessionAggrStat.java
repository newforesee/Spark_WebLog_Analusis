package top.newforesee.dao.session;

import top.newforesee.bean.session.SessionAggrStat;

/**
 * creat by newforesee 2018/11/27
 */
public interface ISessionAggrStat {
    /**
     * 将session聚合统计实例保存到db中
     *
     * @param bean
     */
    void saveBeanToDB(SessionAggrStat bean);
}
