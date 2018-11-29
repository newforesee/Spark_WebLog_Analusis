package top.newforesee.bean.session;

import lombok.Data;

/**
 * creat by newforesee 2018/11/28
 */
@Data
public class Top10CategorySession {
    private int task_id;
    private int category_id;
    private String session_id;
    private int click_count;

    public Top10CategorySession(int task_id, int category_id, String session_id, int click_count) {
        this.task_id = task_id;
        this.category_id = category_id;
        this.session_id = session_id;
        this.click_count = click_count;
    }
}
