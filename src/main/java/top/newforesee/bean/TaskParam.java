package top.newforesee.bean;

import lombok.Data;

import java.util.List;

/**
 * TaskParam实体类
 * creat by newforesee 2018/11/27
 *
 */
@Data
public class TaskParam {
    /**
     * 年龄
     */
    private List<Integer> ages;
    /**
     * 性别
     */
    private List<String> genders;

    /**
     * 职业
     */
    private List<String> professionals;
    /**
     * 城市
     */
    private List<String> cities;

}
