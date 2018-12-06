package top.newforesee.dao.goods.impl;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import top.newforesee.bean.goods.CityInfo;
import top.newforesee.dao.goods.ICityInfo;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * creat by newforesee 2018/11/30
 */
public class CityInfoImpl implements ICityInfo {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public List<CityInfo> findAllInfo() {
        try {
            return qr.query("select * from city_info",new BeanListHandler<>(CityInfo.class));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
