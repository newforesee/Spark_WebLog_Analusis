package top.newforesee.bean.ad;

import java.io.Serializable;
import java.util.Objects;

/**
 * Description：对某个广告点击超过100次的黑名单用户封装实体类<br/>
 * Copyright (c) ， 2018， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author 徐文波
 * @version : 1.0
 */
public class AdBlackList implements Serializable {
    /**
     * 用户编号
     */
    private int user_id;

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }


    public AdBlackList() {
    }

    public AdBlackList(int user_id) {
        this.user_id = user_id;
    }

    //定制AdBlackList实例的相等规则
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AdBlackList that = (AdBlackList) o;
        return user_id == that.user_id;
    }

    @Override
    public int hashCode() {

        return Objects.hash(user_id);
    }

    @Override
    public String toString() {
        return "AdBlackList{" +
                "user_id=" + user_id +
                '}';
    }
}
