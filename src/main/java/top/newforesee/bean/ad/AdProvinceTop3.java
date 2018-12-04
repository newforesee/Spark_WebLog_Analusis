package top.newforesee.bean.ad;

/**
 * xxx
 * creat by newforesee 2018/12/4
 */
public class AdProvinceTop3 {

    /**
     * 日期（每天）
     */
    private String date;

    /**
     * 省份
     */
    private String province;

    /**
     * 广告编号
     */
    private int ad_id;

    public AdProvinceTop3() {
    }

    public AdProvinceTop3(String date, String province, int ad_id) {
        this.date = date;
        this.province = province;
        this.ad_id = ad_id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public int getAd_id() {
        return ad_id;
    }

    public void setAd_id(int ad_id) {
        this.ad_id = ad_id;
    }
}
