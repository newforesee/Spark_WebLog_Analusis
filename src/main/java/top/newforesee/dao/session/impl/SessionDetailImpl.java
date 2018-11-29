package top.newforesee.dao.session.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.session.SessionDetail;
import top.newforesee.dao.session.ISessionDetail;
import top.newforesee.utils.DBCPUtil;

/**
 * creat by newforesee 2018/11/28
 */
public class SessionDetailImpl implements ISessionDetail {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());
    @Override
    public void saveToDB(SessionDetail bean) {
        String sql = "insert into session_deyail values(?,?,?,?,?)";
        // TODO: 2018/11/28
    }
}
