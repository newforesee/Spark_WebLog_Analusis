package top.newforesee.utils;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;


import org.apache.commons.dbcp.BasicDataSourceFactory;
import top.newforesee.constants.Constants;
import top.newforesee.constants.DeployMode;


/**
 * Description： 关于DBCP和C3P0的说明,这两个都是常见的数据库连接池技术<br/>
 * Copyright (c) ， 2018， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author 徐文波
 * @version : 1.0
 */
public class DBCPUtil {
    private static DataSource ds;

    static {
        try {

            Properties properties = new Properties();

            /**
             * 根据conf.properties中的配置信息来判断是执行本地 测试还是生产环境
             */
            DeployMode runMode = ResourcesUtils.dMode;
            String filePath = runMode.toString().toLowerCase()+ File.separator+ResourcesUtils.getPropertyValueByKey(Constants.DBCP_CONFIG_FILE);
            InputStream in = DBCPUtil.class.getClassLoader().getResourceAsStream(filePath);
            properties.load(in);//装在配置文件

            ds = BasicDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {//初始化异常

            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * 获得数据源（连接池）
     *
     * @return
     */
    public static DataSource getDataSource() {
        return ds;
    }

    /**
     * 从连接池中获得连接的实例
     *
     * @return
     */
    public static Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
