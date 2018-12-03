package top.newforesee.dao.ad.impl;


import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import top.newforesee.bean.ad.AdBlackList;
import top.newforesee.dao.ad.IAdBlackListDao;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Description：对某个广告点击超过100次的黑名单用户操作dao层接口实现类<br/>
 * Copyright (c) ， 2018， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author 徐文波
 * @version : 1.0
 */
public class AdBlackListDaoImpl implements IAdBlackListDao {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public List<AdBlackList> findAllAdBlackList() {
        try {
            String sql = "select user_id from ad_blacklist ";
            //获得表中旧的黑名单信息
            List<AdBlackList> oldBlackList = qr.query(sql, new BeanListHandler<AdBlackList>(AdBlackList.class));
            return oldBlackList;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("获取说有黑名单信息发生异常了哦！...");
        }
    }

    @Override
    public void updateBatch(List<AdBlackList> beans) {
        try {
            //步骤：
            //①准备容器，用于存储新增的黑名单信息
            List<AdBlackList> insertContainer = new LinkedList<>();

            //②填充容器
            List<AdBlackList> oldBlackList = findAllAdBlackList();

            //将旧的黑名单列表与待分析的容器中的数据进行比对，若不存在，证明是新增的黑名单，将其添加到容器中即可
            for (AdBlackList bean : beans) {
                if (!oldBlackList.contains(bean)) {
                    insertContainer.add(bean);
                }
            }

            //将新的黑名单批量保存到表中
           String sql = "insert into ad_blacklist values(?)";
            Object[][] params = new Object[insertContainer.size()][];
            for (int i = 0; i < params.length; i++) {
                AdBlackList bean = insertContainer.get(i);
                params[i] = new Object[]{bean.getUser_id()};
            }
            qr.batch(sql, params);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
