package top.newforesee.bean;

/**
 * Task实体类
 * creat by newforesee 2018/11/27
 */

public class Task {
//    `task_id` int(11) NOT NULL AUTO_INCREMENT,
//  `task_name` varchar(255) DEFAULT NULL,
//  `create_time` varchar(255) DEFAULT NULL,
//  `start_time` varchar(255) DEFAULT NULL,
//  `finish_time` varchar(255) DEFAULT NULL,
//  `task_type` varchar(255) DEFAULT NULL,
//  `task_status` varchar(255) DEFAULT NULL,
//  `task_param` text,
    /**
     * 任务编号
     */
    private int task_id;
    //任务名
    private String task_name;
    //创建时间
    private String create_time;
    //开始时间
    private String start_time;
    //结束时间
    private String finish_time;
    //任务类型
    private String task_type;
    //
    private String task_status;
    private String task_param;



}
