package com.hadoop.hbase;

import com.hadoop.hbase.common.Result;
import com.hadoop.hbase.common.ResultUtil;
import com.hadoop.hbase.exception.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 全局异常处理
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {
    /**
     * 这里表示Controller抛出的Exception异常由这个方法处理
     * 响应状态码500
     *
     * @param exception
     * @return
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Result<Object> exceptionHandler(Exception exception) {
        log.error("----application error------", exception);
        return ResultUtil.failureDefaultError();
    }

    /**
     * 这里表示Controller抛出的MethodArgumentNotValidException异常由这个方法处理
     *
     * @param exception
     * @return
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public Result<Object> exceptionHandler(MethodArgumentNotValidException exception) {
        log.error("---request params error----", exception);
        return ResultUtil.failureDefaultError();
    }

    /**
     * 这里表示Controller抛出的MCustomException异常由这个方法处理
     *
     * @param exception
     * @return
     */
    @ExceptionHandler(CustomException.class)
    public Result<Object> exceptionHandler(CustomException exception) {
        log.error("---Custom Exception----", exception);
        return ResultUtil.failure(exception.getException().getCode(), exception.getException().getMessage());
    }
}
