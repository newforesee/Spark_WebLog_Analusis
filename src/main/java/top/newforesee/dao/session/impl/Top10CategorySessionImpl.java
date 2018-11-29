package top.newforesee.dao.session.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.session.Top10CategorySession;
import top.newforesee.dao.session.ITop10CategorySession;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;

/**
 * creat by newforesee 2018/11/29
 */
public class Top10CategorySessionImpl implements ITop10CategorySession {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public void saveBeanToDB(Top10CategorySession bean) {
        String sql = "insert into top10_category_session values(?,?,?,?)";
        try {
            qr.update(sql, bean.getTask_id(), bean.getCategory_id(), bean.getSession_id(), bean.getClick_count());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
