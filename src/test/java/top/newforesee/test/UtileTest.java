package top.newforesee.test;

import org.junit.Test;
import top.newforesee.constants.Constants;
import top.newforesee.utils.DBCPUtil;
import top.newforesee.utils.ResourcesUtils;

import java.sql.Connection;

/**
 * creat by newforesee 2018/11/26
 */
public class UtileTest {
    @Test
    public void testResourcesUtils(){
        System.out.println(ResourcesUtils.getPropertyValueByKey(Constants.SPARK_JOB_DEPLOY_MODE));

    }
    @Test
    public void testDBCPUtil(){
        Connection connection = DBCPUtil.getConnection();
        System.out.println(connection);
    }

}
