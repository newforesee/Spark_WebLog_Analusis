package top.newforesee.dao.ad.impl;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import top.newforesee.bean.ad.AdProvinceTop3;
import top.newforesee.bean.ad.AdProvinceTop3Temp;
import top.newforesee.dao.ad.IAdProvinceTop3Dao;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * xxx
 * creat by newforesee 2018/12/4
 */
public class AdProvinceTop3DaoImpl implements IAdProvinceTop3Dao {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());

    @Override
    public void updateBatch(List<AdProvinceTop3> beans) {
        //步骤：
        //①准备两个容器分别存储要更新的AdUserClickCount实例和要插入的AdUserClickCount实例
        LinkedList<AdProvinceTop3> updateContainer = new LinkedList<>();
        LinkedList<AdProvinceTop3> insertContainer = new LinkedList<>();
        //②填充容器（一次与db中的记录进行比对，若存在，就添加到更新容器中；否则，添加到保存的容器中）
        String sql = "select click_count from ad_province_top3 where `date`=? and province=? and ad_id=?";

        try {
            for (AdProvinceTop3 bean : beans) {
                Object click_count = qr.query(sql, new ScalarHandler<>("click_count"), bean.getDate(), bean.getProvince(), bean.getAd_id());
                if (click_count == null) {
                    insertContainer.add(bean);
                } else {
                    updateContainer.add(bean);
                }
            }
            //③对更新的容器进行批量update操作
            // click_count=click_count+?  <~ ? 证明?传过来的是本batch新增的click_count,不包括过往的历史  (调用处调用：reduceByKey)
            // click_count=?  <~ ? 证明?传过来的是总的click_count （调用出：使用了updateStateByKey）
            sql = "update ad_province_top3 set click_count=?  where `date`=? and province=? and ad_id=?";
            Object[][] params = new Object[updateContainer.size()][];
            for (int i = 0; i < params.length; i++) {
                AdProvinceTop3 bean = updateContainer.get(i);
                params[i] = new Object[]{bean.getClick_count(), bean.getDate(), bean.getProvince(), bean.getAd_id()};
            }
            qr.batch(sql, params);
            //④对保存的容器进行批量insert操作
            saveToDB(insertContainer);
            //对db中已经保存的数据进行筛选（只选出表中相同省份的前三条）
            sql="select * from ad_province_top3 order by province click_count desc";
            List<AdProvinceTop3> allBean = qr.query(sql, new BeanListHandler<AdProvinceTop3>(AdProvinceTop3.class));

            StringBuilder builder = new StringBuilder();
            builder.append("SELECT province ,(adIdTypeCnt-3) delCnt FROM ( ");
            builder.append("SELECT COUNT(*) adIdTypeCnt, province FROM ");
            builder.append("(SELECT * FROM ad_province_top3 ORDER BY province,click_count DESC) t ");
            builder.append("GROUP BY province ");
            builder.append(")t2 WHERE adIdTypeCnt>3");
            //存储了相应省份要删除的多余的记录条数
            List<AdProvinceTop3Temp> willDelBeans = qr.query(builder.toString(), new BeanListHandler<AdProvinceTop3Temp>(AdProvinceTop3Temp.class));

            //真正要删除的记录
            List<AdProvinceTop3> realDelBeans = new LinkedList<>();

            //存储每个省份对应的信息
            List<AdProvinceTop3> perProvince = new LinkedList<>();

            //找出要删除的bean
            for (AdProvinceTop3Temp delBean : willDelBeans) {
                //清空容器
                perProvince.clear();

                String provinceName = delBean.getProvince();
                int delCnt = delBean.getDelCnt();
                for (AdProvinceTop3 beanTmp : allBean) {
                    if (provinceName != null && provinceName.equals(beanTmp.getProvince())) {
                        perProvince.add(beanTmp);
                    }
                }

                //从当前省份对应的容器中筛选出待删除的省份信息，再保存到最终待删除的容器中
                for (int i = 3; i < perProvince.size(); i++) {
                    realDelBeans.add(perProvince.get(i));
                }
            }

            //清空表
            for (AdProvinceTop3 realDelBean : realDelBeans) {
                qr.update("delete from ad_province_top3 where  `date`=? and province=? and ad_id=?",realDelBean.getDate(), realDelBean.getProvince(), realDelBean.getAd_id());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 保存到DB中
     *
     * @param insertContainer 容器
     * @throws SQLException 可能会SQLException
     */
    private void saveToDB(LinkedList<AdProvinceTop3> insertContainer) throws SQLException {
        String sql;
        Object[][] params;
        sql = "insert into ad_province_top3 values(?,?,?,?)";
        params = new Object[insertContainer.size()][];
        for (int i = 0; i < params.length; i++) {
            AdProvinceTop3 bean = insertContainer.get(i);
            params[i] = new Object[]{bean.getDate(), bean.getProvince(), bean.getAd_id(), bean.getClick_count()};
        }
        qr.batch(sql, params);
    }


}














