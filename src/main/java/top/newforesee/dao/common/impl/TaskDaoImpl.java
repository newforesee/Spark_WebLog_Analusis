package top.newforesee.dao.common.impl;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import top.newforesee.bean.Task;
import top.newforesee.dao.common.ITaskDao;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;

/**
 * creat by newforesee 2018/11/27
 */
public class TaskDaoImpl implements ITaskDao {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());
    @Override
    public Task findTaskById(int taskId) {
        try {
            return qr.query("select * from task where task_id=?",new BeanHandler<Task>(Task.class),taskId);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
