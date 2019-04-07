package top.newforesee.bean;

import lombok.Data;

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

    public Task() {
    }

    public int getTask_id() {
        return task_id;
    }

    public void setTask_id(int task_id) {
        this.task_id = task_id;
    }

    public String getTask_name() {
        return task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getFinish_time() {
        return finish_time;
    }

    public void setFinish_time(String finish_time) {
        this.finish_time = finish_time;
    }

    public String getTask_type() {
        return task_type;
    }

    public void setTask_type(String task_type) {
        this.task_type = task_type;
    }

    public String getTask_status() {
        return task_status;
    }

    public void setTask_status(String task_status) {
        this.task_status = task_status;
    }

    public String getTask_param() {
        return task_param;
    }

    public void setTask_param(String task_param) {
        this.task_param = task_param;
    }

}
