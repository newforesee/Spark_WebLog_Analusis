package top.newforesee.dao.ad.impl;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import top.newforesee.bean.ad.AdUserClickCount;
import top.newforesee.dao.ad.IADUserClickCountDao;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * 统计每天各用户对各广告的点击次数功能接口实现类
 * creat by newforesee 2018/11/30
 */
public class ADUserClickCountDaoImpl implements IADUserClickCountDao {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public void updateBatch(List<AdUserClickCount> beans) {
        //①准备两个容器分别存储要更新的AdUserClickCount实例和要插入的AdUserClickCount实例
        LinkedList<AdUserClickCount> updateContainer = new LinkedList<>();
        LinkedList<AdUserClickCount> insertContainer = new LinkedList<>();
        try {
            //②填充容器（一次与db中的记录进行比对，若存在，就添加到更新容器中；否则，添加到保存的容器中）
            String sql = "select click_count from ad_user_click_count where `date`=? and user_id=? and ad_id=?";
            for (AdUserClickCount bean : beans) {
                //ScalarHandler:用于统计表记录的条数
                //BeanHandler:用来将表中每条记录封装到一个实例中
                //BeanListHandler: 用来将表中所有记录封装到一个集合中，集合中每个元素即为：每条记录所封装的实体类对象

                Object click_count = qr.query(sql, new ScalarHandler<>("click_count"), bean.getDate(), bean.getUser_id(), bean.getAd_id());
                if (click_count == null) {
                    insertContainer.add(bean);
                } else {
                    updateContainer.add(bean);
                }
            }

                //③对更新的容器进行批量update操作
                // click_count=click_count+?  <~ ? 证明?传过来的是本batch新增的click_count,不包括过往的历史  (调用处调用：reduceByKey)
                // click_count=?  <~ ? 证明?传过来的是总的click_count （调用出：使用了updateStateByKey）

                sql = "update ad_user_click_count set click_count=? where `date`=? and user_id=? and ad_id=?";
                Object[][] params = new Object[updateContainer.size()][];
                for (int i = 0; i < params.length; i++) {
                    AdUserClickCount bean = updateContainer.get(i);
                    params[i] = new Object[]{bean.getClick_count(), bean.getDate(), bean.getUser_id(), bean.getAd_id()};

                }
                qr.batch(sql,params);

            //④对保存的容器进行批量insert操作
            sql = "insert into ad_user_click_count values(?,?,?,?)";
            params = new Object[insertContainer.size()][];
            for (int i = 0; i < params.length; i++) {
                AdUserClickCount bean = insertContainer.get(i);
                params[i] = new Object[]{bean.getDate(), bean.getUser_id(), bean.getAd_id(), bean.getClick_count()};
            }
            qr.batch(sql, params);



        } catch (SQLException e1) {
            e1.printStackTrace();
        }

    }
}
