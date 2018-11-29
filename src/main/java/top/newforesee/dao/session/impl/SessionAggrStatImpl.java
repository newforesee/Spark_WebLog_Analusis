package top.newforesee.dao.session.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.session.SessionAggrStat;
import top.newforesee.dao.session.ISessionAggrStat;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;

/**
 * creat by newforesee 2018/11/27
 */
public class SessionAggrStatImpl implements ISessionAggrStat {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());
    @Override
    public void saveBeanToDB(SessionAggrStat bean) {
        try {
            qr.update("insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    bean.getTask_id(),
                    bean.getSession_count(),
                    bean.getPeriod_1s_3s(),
                    bean.getPeriod_4s_6s(),
                    bean.getPeriod_7s_9s(),
                    bean.getPeriod_10s_30s(),
                    bean.getPeriod_30s_60s(),
                    bean.getPeriod_1m_3m(),
                    bean.getPeriod_3m_10m(),
                    bean.getPeriod_10m_30m(),
                    bean.getPeriod_30m(),
                    bean.getStep_1_3(),
                    bean.getStep_4_6(),
                    bean.getStep_7_9(),
                    bean.getStep_10_30(),
                    bean.getStep_30_60(),
                    bean.getStep_60());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
