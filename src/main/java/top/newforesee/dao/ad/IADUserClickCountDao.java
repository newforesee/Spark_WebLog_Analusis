package top.newforesee.dao.ad;

import top.newforesee.bean.ad.AdUserClickCount;

import java.util.List;

/**
 * 统计每天各用户对各广告的点击次数功能接口
 * creat by newforesee 2018/11/30
 */
public interface IADUserClickCountDao {
    /**
     * 批量更新（包括两步骤：①批量更新；②批量保存）
     */
    void updateBatch(List<AdUserClickCount> beans);
}
