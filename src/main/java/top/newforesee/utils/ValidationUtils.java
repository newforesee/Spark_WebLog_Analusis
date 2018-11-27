package top.newforesee.utils;

/**
 * Description：校验工具类<br/>
 * Copyright (c) ， 2018， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author 徐文波
 * @version : 1.0
 */
public class ValidationUtils {

    /**
     * 校验数据中的指定字段，是否在指定范围内
     *
     * @param data      数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @return 校验结果
     */
    public static boolean between(String data, String dataField,
                                  String parameter, String delimiter) {
        return data != null && data.trim().equals(StringUtils.getFieldFromConcatString(parameter, delimiter, dataField));
    }
}
