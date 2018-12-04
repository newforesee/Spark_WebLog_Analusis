package top.newforesee.dao.ad;

import top.newforesee.bean.ad.AdProvinceTop3;

import java.util.List;

/**
 * 每天各省份top3热门广告的数据处理Dao层接口
 * creat by newforesee 2018/12/4
 */
public interface IAdProvinceTop3Dao {
    /**
     * 批量更新
     */
    void updateBatch(List<AdProvinceTop3> beans);
}
