package top.newforesee.bean.session;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * creat by newforesee 2018/11/28
 */
@Data
@AllArgsConstructor
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

}
