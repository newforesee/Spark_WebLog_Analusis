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

    /**
     * 点击次数
     */
    private int click_count;

    public AdProvinceTop3() {
    }

    public AdProvinceTop3(String date, String province, int ad_id, int click_count) {
        this.date = date;
        this.province = province;
        this.ad_id = ad_id;
        this.click_count = click_count;
    }

    public int getClick_count() {
        return click_count;
    }

    public void setClick_count(int click_count) {
        this.click_count = click_count;
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
