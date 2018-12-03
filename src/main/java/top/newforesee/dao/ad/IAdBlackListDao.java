package top.newforesee.dao.ad;


import top.newforesee.bean.ad.AdBlackList;

import java.util.List;

/**
 * Description：对某个广告点击超过100次的黑名单用户操作dao层接口<br/>
 * Copyright (c) ， 2018， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 *
 * @author 徐文波
 * @version : 1.0
 */
public interface IAdBlackListDao {

    /**
     * 查询所有黑名单信息
     *
     * @return
     */
    List<AdBlackList> findAllAdBlackList();

    /**
     * 批量更新黑名单（保存）
     */
    void updateBatch(List<AdBlackList> beans);
}
