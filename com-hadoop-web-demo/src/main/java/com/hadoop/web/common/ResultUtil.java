package com.hadoop.web.common;


import org.springframework.util.StringUtils;

public class ResultUtil {

    private static final int SUCCESS_CODE = 200;
    private static final int FAILURE_CODE = 500;
    private static final String BASE_ERROR_CODE = "BASE-SYS-ERR-0";
    private static final String BASE_ERROR_MSG = "网络繁忙，请稍后再试";

    /**
     * 返回成功result
     *
     * @param <T>
     * @return
     */
    public static <T> Result<T> success() {
        return success(null);
    }

    /**
     * 返回成功result
     *
     * @param data
     * @param <T>
     * @return
     */
    public static <T> Result<T> success(T data) {
        return new Result<>(SUCCESS_CODE, null, null, data);
    }


    /**
     * 返回失败result
     *
     * @param <T>
     * @return
     */
    public static <T> Result<T> failureDefaultError() {
        return failure(BASE_ERROR_CODE, BASE_ERROR_MSG, null);
    }


    /**
     * 返回失败result
     *
     * @param <T>
     * @return
     */
    public static <T> Result<T> failure(String errCode) {
        return failure(errCode, null, null);
    }


    /**
     * 返回失败result
     *
     * @param <T>
     * @return
     */
    public static <T> Result<T> failure(String errCode, String errMsg) {
        return failure(errCode, errMsg, null);
    }

    /**
     * 返回失败result
     *
     * @param data
     * @param <T>
     * @return
     */
    public static <T> Result<T> failure(String errCode, String errMsg, T data) {
        return getFailResult(errCode, errMsg, data);
    }

    /**
     * 返回失败result
     *
     * @param errMsg
     * @param data
     * @param <T>
     * @return
     */
    public static <T> Result<T> failure(String errMsg, T data) {
        return getFailResult(BASE_ERROR_CODE, errMsg, data);
    }

    /**
     * 获取错误码和错误信息，返回result对象
     *
     * @param errCode
     * @param errMsg
     * @param data
     * @param <T>
     * @return
     */
    private static <T> Result<T> getFailResult(String errCode, String errMsg, T data) {
        if (StringUtils.isEmpty(errCode)) {
            if (StringUtils.isEmpty(errMsg)) {
                //获取msg
                if (StringUtils.isEmpty(errMsg)) {
                    errMsg = BASE_ERROR_MSG;
                }
            }
        } else {
            errCode = BASE_ERROR_CODE;
            errMsg = BASE_ERROR_MSG;
        }
        return new Result<>(FAILURE_CODE, errCode, errMsg, data);
    }
}
