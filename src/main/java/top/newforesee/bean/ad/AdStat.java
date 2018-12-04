package top.newforesee.bean.ad;

/**
 * creat by newforesee 2018/12/4
 */
public class AdStat {
    /**
     * 日期（天）
     */
    private String date;

    /**
     * 省份
     */
    private String province;

    /**
     * 城市
     */
    private String city;
    /**
     * 广告编号
     */
    private int ad_id;

    /**
     *点击次数
     */
    private int click_count;

    public AdStat() {
    }

    public AdStat(String date, String province, String city, int ad_id, int click_count) {
        this.date = date;
        this.province = province;
        this.city = city;
        this.ad_id = ad_id;
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

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getAd_id() {
        return ad_id;
    }

    public void setAd_id(int ad_id) {
        this.ad_id = ad_id;
    }

    public int getClick_count() {
        return click_count;
    }

    public void setClick_count(int click_count) {

        this.click_count = click_count;
    }

    @Override
    public String toString() {
        return "AdStat{" +
                "date='" + date + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", ad_id=" + ad_id +
                ", click_count=" + click_count +
                '}';
    }
}
