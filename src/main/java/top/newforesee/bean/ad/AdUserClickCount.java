package top.newforesee.bean.ad;

/**
 * creat by newforesee 2018/11/30
 */
public class AdUserClickCount {
    /**
     * 每天
     */
    private String date;

    /**
     * 用户编号
     */
    private int user_id;

    /**
     * 广告编号
     */
    private int ad_id;

    /**
     * 点击次数
     */
    private int click_count;

    public AdUserClickCount() {
    }

    public AdUserClickCount(String date, int user_id, int ad_id, int click_count) {
        this.date = date;
        this.user_id = user_id;
        this.ad_id = ad_id;
        this.click_count = click_count;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
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
}
