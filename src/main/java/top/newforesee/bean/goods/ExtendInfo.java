package top.newforesee.bean.goods;

import lombok.Data;

/**
 * creat by newforesee 2018/11/30
 */
@Data
public class ExtendInfo {
    /**
     * 产品状态码
     */
    private int product_status;

    public ExtendInfo(int product_status) {
        this.product_status = product_status;
    }
}
