package top.newforesee.dao.common;

import top.newforesee.bean.Task;

/**
 * creat by newforesee 2018/11/27
 */
public interface ITaskDao {
    Task findTaskById(String taskId);
}
