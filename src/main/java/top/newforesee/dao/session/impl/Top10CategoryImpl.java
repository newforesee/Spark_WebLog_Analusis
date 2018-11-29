package top.newforesee.dao.session.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.session.Top10Category;
import top.newforesee.dao.session.ITop10Category;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;

/**
 * 操作特定品类点击、下单和支付总数对应的实体类的数据访问层接口实现类<br/>
 * creat by newforesee 2018/11/29
 */
public class Top10CategoryImpl implements ITop10Category {

    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public void saveBeanToDB(Top10Category bean) {
        String sql = "insert into top10_category values(?,?,?,?,?)";
        try {
            qr.update(sql, bean.getTask_id(), bean.getCategory_id(), bean.getClick_count(), bean.getOrder_count(), bean.getPay_count());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
