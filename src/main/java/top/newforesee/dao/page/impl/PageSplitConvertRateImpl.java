package top.newforesee.dao.page.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.page.PageSplitConvertRate;
import top.newforesee.dao.page.IPageSplitConvertRate;
import top.newforesee.utils.DBCPUtil;

/**
 * creat by newforesee 2018/11/30
 */
public class PageSplitConvertRateImpl implements IPageSplitConvertRate {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public void saveToDB(PageSplitConvertRate bean) {

    }
}
