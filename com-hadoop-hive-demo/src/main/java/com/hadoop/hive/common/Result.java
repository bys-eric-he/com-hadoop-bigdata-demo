package com.hadoop.hive.common;


import lombok.Data;

/**
 * API 统一返回结果
 *
 * @param <T>
 */
@Data
public class Result<T> {

    /**
     * 响应码
     */
    private Integer resCode;

    /**
     * 错误码
     */
    private String errCode;

    /**
     * 错误信息
     */
    private String errMsg;

    /**
     * 数据
     */
    private T data;

    public Result(Integer resCode, String errCode, String errMsg, T data) {
        this.resCode = resCode;
        this.errCode = errCode;
        this.errMsg = errMsg;
        this.data = data;
    }
}