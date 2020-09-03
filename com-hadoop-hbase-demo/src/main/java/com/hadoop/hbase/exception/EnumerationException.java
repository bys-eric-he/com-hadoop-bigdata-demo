package com.hadoop.hbase.exception;

/**
 * 异常枚举类
 */
public enum EnumerationException {
    PARAMETER_ERROR("10100", "系统异常，入参错误!"),
    SYSTEM_ERROR("10001", "系统异常，系统内部异常!");

    private String code;
    private String message;

    private EnumerationException(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
