package top.newforesee.dao.goods;

import top.newforesee.bean.goods.HotGoodsInfo;

import java.util.List;

/**
 * creat by newforesee 2018/11/30
 */
public interface IHotGoodsInfoDao {
    /**
     * 将参数指定的集合保存到db中
     * @param beans beans
     */
    void saveBeansToDB(List<HotGoodsInfo> beans);
}
