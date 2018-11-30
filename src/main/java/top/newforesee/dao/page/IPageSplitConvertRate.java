package top.newforesee.dao.page;

import top.newforesee.bean.page.PageSplitConvertRate;

/**
 * 页面单挑转化率数据访问层接口
 * creat by newforesee 2018/11/30
 */
public interface IPageSplitConvertRate {

    /**
     *
     * @param bean
     */
    void saveToDB(PageSplitConvertRate bean);
}
