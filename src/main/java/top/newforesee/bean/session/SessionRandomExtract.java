package top.newforesee.bean.session;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * creat by newforesee 2018/11/28
 */
@Data
public class SessionRandomExtract {
    /**
     * 任务编号
     */
    private int task_id;
    /**
     * session_id
     */
    private String session_id;
    /**
     * session开始时间
     */
    private String start_time;
    /**
     * 结束时间
     */
    private String end_time;
    /**
     * 搜索的关键词
     */
    private String search_keywords;

    public SessionRandomExtract() {
    }

    public SessionRandomExtract(int task_id, String session_id, String start_time, String end_time, String search_keywords) {
        this.task_id = task_id;
        this.session_id = session_id;
        this.start_time = start_time;
        this.end_time = end_time;
        this.search_keywords = search_keywords;
    }
}
