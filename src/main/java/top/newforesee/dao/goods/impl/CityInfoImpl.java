package top.newforesee.dao.goods.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.goods.CityInfo;
import top.newforesee.dao.goods.ICityInfo;
import top.newforesee.utils.DBCPUtil;

import java.util.List;

/**
 * creat by newforesee 2018/11/30
 */
public class CityInfoImpl implements ICityInfo {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public List<CityInfo> findAllInfo() {
        return null;
    }
}
