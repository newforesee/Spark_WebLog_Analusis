package top.newforesee.dao.session;

import top.newforesee.bean.session.SessionDetail;

/**
 * session明细数据数据访问层接口
 * creat by newforesee 2018/11/28
 */
public interface ISessionDetail {
    void saveToDB(SessionDetail bean);
}
