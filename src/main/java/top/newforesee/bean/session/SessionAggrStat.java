package top.newforesee.bean.session;

import lombok.Data;

/**
 * creat by newforesee 2018/11/27
 */
@Data
public class SessionAggrStat {
    private int task_id;
    private int session_count;
    private double period_1s_3s;
    private double period_4s_6s;
    private double period_7s_9s;
    private double period_10s_30s;
    private double period_30s_60s;
    private double period_1m_3m;
    private double period_3m_10m;
    private double period_10m_30m;
    private double period_30m;
    private double step_1_3;
    private double step_4_6;
    private double step_7_9;
    private double step_10_30;
    private double step_30_60;
    private double step_60;

    public SessionAggrStat(int task_id, int session_count, double period_1s_3s, double period_4s_6s, double period_7s_9s, double period_10s_30s, double period_30s_60s, double period_1m_3m, double period_3m_10m, double period_10m_30m, double period_30m, double step_1_3, double step_4_6, double step_7_9, double step_10_30, double step_30_60, double step_60) {
        this.task_id = task_id;
        this.session_count = session_count;
        this.period_1s_3s = period_1s_3s;
        this.period_4s_6s = period_4s_6s;
        this.period_7s_9s = period_7s_9s;
        this.period_10s_30s = period_10s_30s;
        this.period_30s_60s = period_30s_60s;
        this.period_1m_3m = period_1m_3m;
        this.period_3m_10m = period_3m_10m;
        this.period_10m_30m = period_10m_30m;
        this.period_30m = period_30m;
        this.step_1_3 = step_1_3;
        this.step_4_6 = step_4_6;
        this.step_7_9 = step_7_9;
        this.step_10_30 = step_10_30;
        this.step_30_60 = step_30_60;
        this.step_60 = step_60;
    }


}
