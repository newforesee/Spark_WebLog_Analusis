package top.newforesee.bean.goods;

import lombok.Data;

/**
 * creat by newforesee 2018/11/30
 */

public class HotGoodsInfo {
    private int task_id;
    private String area;
    private String area_level;
    private int product_id;
    private String city_names;
    private int click_count;
    private String product_name;
    private String product_status;

    public HotGoodsInfo() {
    }

    public HotGoodsInfo(int task_id, String area, String area_level, int product_id, String city_names, int click_count, String product_name, String product_status) {
        this.task_id = task_id;
        this.area = area;
        this.area_level = area_level;
        this.product_id = product_id;
        this.city_names = city_names;
        this.click_count = click_count;
        this.product_name = product_name;
        this.product_status = product_status;
    }

    public int getTask_id() {
        return task_id;
    }

    public void setTask_id(int task_id) {
        this.task_id = task_id;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getArea_level() {
        return area_level;
    }

    public void setArea_level(String area_level) {
        this.area_level = area_level;
    }

    public int getProduct_id() {
        return product_id;
    }

    public void setProduct_id(int product_id) {
        this.product_id = product_id;
    }

    public String getCity_names() {
        return city_names;
    }

    public void setCity_names(String city_names) {
        this.city_names = city_names;
    }

    public int getClick_count() {
        return click_count;
    }

    public void setClick_count(int click_count) {
        this.click_count = click_count;
    }

    public String getProduct_name() {
        return product_name;
    }

    public void setProduct_name(String product_name) {
        this.product_name = product_name;
    }

    public String getProduct_status() {
        return product_status;
    }

    public void setProduct_status(String product_status) {
        this.product_status = product_status;
    }

    @Override
    public String toString() {
        return "HotGoodsInfo{" +
                "task_id=" + task_id +
                ", area='" + area + '\'' +
                ", area_level='" + area_level + '\'' +
                ", product_id=" + product_id +
                ", city_names='" + city_names + '\'' +
                ", click_count=" + click_count +
                ", product_name='" + product_name + '\'' +
                ", product_status='" + product_status + '\'' +
                '}';
    }
}
