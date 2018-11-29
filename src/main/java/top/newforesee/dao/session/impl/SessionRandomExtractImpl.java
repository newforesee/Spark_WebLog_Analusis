package top.newforesee.dao.session.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.session.SessionRandomExtract;
import top.newforesee.dao.session.ISessionRandomExtract;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * creat by newforesee 2018/11/28
 */
public class SessionRandomExtractImpl implements ISessionRandomExtract {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());
    @Override
    public void saveBeansToDB(List<SessionRandomExtract> beans) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[][] params = new Object[beans.size()][];
        for (int i = 0; i < params.length; i++) {
            SessionRandomExtract bean = beans.get(i);
            params[i] = new Object[]{bean.getTask_id(),bean.getSession_id(),
            bean.getStart_time(),bean.getEnd_time(),bean.getSearch_keywords()};
        }
        try {
            qr.batch(sql,params);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
