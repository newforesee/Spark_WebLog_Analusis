package top.newforesee.test;

import org.junit.Test;
import top.newforesee.bean.TaskParam;
import top.newforesee.dao.common.ITaskDao;
import top.newforesee.dao.common.impl.TaskDaoImpl;

import java.util.List;

/**
 * creat by newforesee 2018/11/27
 */
public class CommonTest {
    @Test
    public void test() {
        ITaskDao dao = new TaskDaoImpl();
        dao.findTaskById(1);
    }

    @Test
    public void testSession() {
        TaskParam param = new TaskParam();
        List<Integer> ages = param.getAges();
        System.out.println(ages);
    }
}
