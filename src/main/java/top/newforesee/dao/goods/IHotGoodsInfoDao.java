package top.newforesee.dao.goods;

import top.newforesee.bean.goods.HotGoodsInfo;

import java.util.List;

/**
 * creat by newforesee 2018/11/30
 */
public interface IHotGoodsInfoDao {
    void saveBeansToDB(List<HotGoodsInfo> beans);
}
