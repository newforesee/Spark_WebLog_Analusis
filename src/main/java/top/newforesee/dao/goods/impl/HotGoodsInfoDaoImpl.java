package top.newforesee.dao.goods.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.goods.HotGoodsInfo;
import top.newforesee.dao.goods.IHotGoodsInfoDao;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * creat by newforesee 2018/11/30
 */
public class HotGoodsInfoDaoImpl implements IHotGoodsInfoDao {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public void saveBeansToDB(List<HotGoodsInfo> beans) {
        String sql = "insert into hot_goods_info values(?,?,?,?,?,?,?,?)";
        Object[][] objs = new Object[beans.size()][];
        for (int i = 0; i < objs.length; i++) {
            HotGoodsInfo bean = beans.get(i);
            objs[i]=new Object[]{
                    bean.getTask_id(),
                    bean.getArea(),
                    bean.getArea_level(),
                    bean.getProduct_id(),
                    bean.getCity_names(),
                    bean.getClick_count(),
                    bean.getProduct_name(),
                    bean.getProduct_status()
            };
        }
        try {
            qr.batch(sql,objs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
