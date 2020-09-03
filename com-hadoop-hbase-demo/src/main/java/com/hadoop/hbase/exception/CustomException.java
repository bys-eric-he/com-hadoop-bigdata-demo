package com.hadoop.hbase.exception;

/**
 * 用户自定义异常
 */
public class CustomException extends Exception {

    private EnumerationException exception;

    public CustomException() {
    }

    public CustomException(EnumerationException exception) {
        super(exception.getMessage());
        this.exception = exception;
    }

    public EnumerationException getException() {
        return exception;
    }

    public void setException(EnumerationException exception) {
        this.exception = exception;
    }
}
