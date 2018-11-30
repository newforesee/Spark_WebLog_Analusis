package top.newforesee.bean.goods;

import lombok.Data;

/**
 * creat by newforesee 2018/11/30
 */
@Data
public class CityInfo {
    private int city_id;
    private String city_name;
    private String area;

    public CityInfo() {
    }

    public CityInfo(int city_id, String city_name, String area) {
        this.city_id = city_id;
        this.city_name = city_name;
        this.area = area;
    }
}
