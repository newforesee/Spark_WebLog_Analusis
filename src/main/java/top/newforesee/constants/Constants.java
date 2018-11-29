package top.newforesee.constants;

/**
 * DBCP_CONFIG_FILE
 * CREAT BY NEWFORESEE 2018/11/26
 */
public interface Constants {
    String SPARK_JOB_DEPLOY_MODE = "spark.job.deploy.mode";
    /**
     * 数据库共通的资源文件名
     */
    String DBCP_CONFIG_FILE="dbcp.config.file";

    /**
     * 不同session数的标识
     */
    String SESSION_COUNT = "session_count";

    /**
     * 共通初始化
     */
    String COMMON_INIT = "=0|";
    String COMMON_INIT_2 = "=0";

    /**
     * 时长标识
     */
    String TIME_PERIOD_1s_3s = "1s_3s";
    String TIME_PERIOD_4s_6s = "4s_6s";
    String TIME_PERIOD_7s_9s = "7s_9s";
    String TIME_PERIOD_10s_30s = "10s_30s";
    String TIME_PERIOD_30s_60s = "30s_60s";
    String TIME_PERIOD_1m_3m = "1m_3m";
    String TIME_PERIOD_3m_10m = "3m_10m";
    String TIME_PERIOD_10m_30m = "10m_30m";
    String TIME_PERIOD_30m = "30m";

    /**
     * 步长标识
     */
    String STEP_PERIOD_1_3 = "1_3";
    String STEP_PERIOD_4_6 = "4_6";
    String STEP_PERIOD_7_9 = "7_9";
    String STEP_PERIOD_10_30 = "10_30";
    String STEP_PERIOD_30_60 = "30_60";
    String STEP_PERIOD_60 = "60";

    /**
     * session聚合统计的结果常量
     */
    StringBuilder AGGR_RESULT = new StringBuilder()
            .append(SESSION_COUNT).append(COMMON_INIT)
            .append(TIME_PERIOD_1s_3s).append(COMMON_INIT)
            .append(TIME_PERIOD_4s_6s).append(COMMON_INIT)
            .append(TIME_PERIOD_7s_9s).append(COMMON_INIT)
            .append(TIME_PERIOD_10s_30s).append(COMMON_INIT)
            .append(TIME_PERIOD_30s_60s).append(COMMON_INIT)
            .append(TIME_PERIOD_1m_3m).append(COMMON_INIT)
            .append(TIME_PERIOD_3m_10m).append(COMMON_INIT)
            .append(TIME_PERIOD_10m_30m).append(COMMON_INIT)
            .append(TIME_PERIOD_30m).append(COMMON_INIT)
            .append(STEP_PERIOD_1_3).append(COMMON_INIT)
            .append(STEP_PERIOD_4_6).append(COMMON_INIT)
            .append(STEP_PERIOD_7_9).append(COMMON_INIT)
            .append(STEP_PERIOD_10_30).append(COMMON_INIT)
            .append(STEP_PERIOD_30_60).append(COMMON_INIT)
            .append(STEP_PERIOD_60).append(COMMON_INIT_2);

    //session_count=0|1s_3s=0|4s_6s.../60=0


    /**
     * 页面流
     */
    String PAGE_FLOW = "1_2=0|2_3=0|3_4=0|4_5=0|5_6=0|6_7=0|7_8=0|8_9=0|9_10=0";


    /**
     * 用户点击广告次数的临界值（>100,就是黑名单用户）
     */
    int MAX_CLICK_CNT=100;
}
