package top.newforesee.bean.session;

import lombok.Data;

/**
 * Session明细数据实体类
 * creat by newforesee 2018/11/28
 */
@Data
public class SessionDetail {
    private int task_id;
    private int user_id;
    private String session_id;
    private int page_id;
    private String action_time;
    private String search_keyword;
    private int click_category_id;
    private int click_product_id;
    private String order_category_ids;
    private String order_product_ids;
    private String pay_category_ids;
    private String pay_product_ids;

    public SessionDetail() {
    }

    public SessionDetail(int task_id, int user_id, String session_id, int page_id, String action_time, String search_keyword, int click_category_id, int click_product_id, String order_category_ids, String order_product_ids, String pay_category_ids, String pay_product_ids) {
        this.task_id = task_id;
        this.user_id = user_id;
        this.session_id = session_id;
        this.page_id = page_id;
        this.action_time = action_time;
        this.search_keyword = search_keyword;
        this.click_category_id = click_category_id;
        this.click_product_id = click_product_id;
        this.order_category_ids = order_category_ids;
        this.order_product_ids = order_product_ids;
        this.pay_category_ids = pay_category_ids;
        this.pay_product_ids = pay_product_ids;
    }
}
