package top.newforesee.bean;

import lombok.Data;

import java.util.List;

/**
 * TaskParam实体类
 * creat by newforesee 2018/11/27
 *
 */

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


    /**
     * 开始时间
     */
    private String start_time;

    /**
     * 结束时间
     */
    private String end_time;


    /**
     * 页面流
     */
    private List<Integer> page_flow;

    public List<Integer> getAges() {
        return ages;
    }

    public void setAges(List<Integer> ages) {
        this.ages = ages;
    }

    public List<String> getGenders() {
        return genders;
    }

    public void setGenders(List<String> genders) {
        this.genders = genders;
    }

    public List<String> getProfessionals() {
        return professionals;
    }

    public void setProfessionals(List<String> professionals) {
        this.professionals = professionals;
    }

    public List<String> getCities() {
        return cities;
    }

    public void setCities(List<String> cities) {
        this.cities = cities;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public List<Integer> getPage_flow() {
        return page_flow;
    }

    public void setPage_flow(List<Integer> page_flow) {
        this.page_flow = page_flow;
    }
}
