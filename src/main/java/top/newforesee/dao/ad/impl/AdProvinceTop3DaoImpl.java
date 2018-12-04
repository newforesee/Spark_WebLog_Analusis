package top.newforesee.dao.ad.impl;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import top.newforesee.bean.ad.AdProvinceTop3;
import top.newforesee.dao.ad.IAdProvinceTop3Dao;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * xxx
 * creat by newforesee 2018/12/4
 */
public class AdProvinceTop3DaoImpl implements IAdProvinceTop3Dao {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());
    @Override
    public void updateBatch(List<AdProvinceTop3> beans) {
        //步骤：
        //①准备两个容器分别存储要更新的AdUserClickCount实例和要插入的AdUserClickCount实例
        LinkedList<AdProvinceTop3> updateContainer = new LinkedList<>();
        LinkedList<AdProvinceTop3> insertContainer = new LinkedList<>();
        //②填充容器（一次与db中的记录进行比对，若存在，就添加到更新容器中；否则，添加到保存的容器中）
        String sql = "select click_count from ad_province_top3 where `date`=? and province=? and ad_id=?";
        for (AdProvinceTop3 bean : beans) {
            try {
                Object click_count = qr.query(sql, new ScalarHandler<>("click_count"), bean.getDate(), bean.getProvince(), bean.getAd_id());

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }
}














