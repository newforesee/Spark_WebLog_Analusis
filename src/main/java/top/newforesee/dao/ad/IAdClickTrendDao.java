package top.newforesee.dao.ad;

import top.newforesee.bean.ad.AdClickTrend;

import java.util.List;

/**
 * 最近1小时各广告各分钟的点击量处理Dao层接口
 * creat by newforesee 2018/12/4
 */
public interface IAdClickTrendDao {
    /**
     * 批量更新
     */
    void updateBatch(List<AdClickTrend> beans);
}
