package top.newforesee.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Description：参数工具类<br/>
 * Copyright (c) ， 2018， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author 徐文波
 * @version : 1.0
 */
public class ParamUtils {

    /**
     * 从命令行参数中提取任务id
     *
     * @param args 命令行参数  ./spark-submit    .jar 1
     * @return 任务id
     */
    public static Long getTaskIdFromArgs(String[] args) {
        if (args != null && args.length > 0) {
            return Long.valueOf(args[0]);
        } else {
            throw new RuntimeException("没有执行任务的id！请明示！");
        }
    }

    /**
     * 从JSON对象中提取参数
     *
     * @param jsonStr JSON对象   {task_id:101,task_name:"用户session日志分析",task_param:[20~28,程序员,xx]}
     * @return 参数
     */
    public static String getParam(String jsonStr, String field) {
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        return jsonObject.getString(field);
    }

}
