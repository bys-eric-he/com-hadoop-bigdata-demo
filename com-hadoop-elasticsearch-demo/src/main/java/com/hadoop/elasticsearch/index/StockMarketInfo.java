package com.hadoop.elasticsearch.index;

import lombok.Data;

import java.io.Serializable;

/**
 * 股票行情数据
 */
@Data
public class StockMarketInfo implements Serializable {

    private static final long serialVersionUID = 8655855566225363655L;
    
    /**
     * 股票代码
     */
    private String stockCode;
    /**
     * 股票名称
     */
    private String stockName;
    /**
     * 总市值
     */
    private String totalMarketValue;
    /**
     * 交易日
     */
    private String tradeDate;
    /**
     * 涨跌幅
     */
    private String upAndDown;
}
