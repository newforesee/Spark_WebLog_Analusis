package top.newforesee.dao.session;

import top.newforesee.bean.session.SessionRandomExtract;

import java.util.List;

/**
 *
 * 按照时间比例随机抽取功能抽取出来session
 * creat by newforesee 2018/11/28
 */
public interface ISessionRandomExtract {
    void  saveBeansToDB(List<SessionRandomExtract> beans);
}
