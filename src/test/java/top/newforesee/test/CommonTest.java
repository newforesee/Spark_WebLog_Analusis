package top.newforesee.test;

import top.newforesee.dao.common.ITaskDao;
import top.newforesee.dao.common.impl.TaskDaoImpl;

/**
 * creat by newforesee 2018/11/27
 */
public class CommonTest {
    public void test() {
        ITaskDao dao = new TaskDaoImpl();
dao.findTaskById(1);
    }

}
