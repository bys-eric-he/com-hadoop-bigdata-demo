package com.hadoop.hbase.controller;


import com.hadoop.hbase.common.Result;
import com.hadoop.hbase.common.ResultUtil;
import com.hadoop.hbase.service.HBaseService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Api(tags = "UserController", description = "Spring Boot 集成 HBase 操作 Demo")
@RestController
@RequestMapping(value = "/api/v1/user")
public class UserController {

    @Autowired
    private HBaseService hBaseService;

    private final Logger logger = LoggerFactory.getLogger(UserController.class);

    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamilies
     * @return
     * @throws Exception
     */
    @PostMapping(path = "/create")
    @ApiOperation("创建表")
    public Result<String> createTable(@RequestParam(value = "tableName") String tableName, String[] columnFamilies) throws Exception {
        try {
            hBaseService.createTable(tableName, columnFamilies);
        } catch (Exception ex) {
            logger.error("创建HBase表{}异常!异常信息->{}", tableName, ex);
            throw ex;
        }

        return ResultUtil.success();
    }

    /**
     * 插入或更新数据
     *
     * @param tableName
     * @param row
     * @param columnFamily
     * @param column
     * @param value
     * @return
     */
    @PostMapping(path = "/insert")
    @ApiOperation("插入或更新数据")
    public Result<String> insertOrUpdate(@RequestParam(value = "tableName") String tableName, String row, String columnFamily, String column, String value) {
        try {

            hBaseService.insertOneRecord(tableName, row, columnFamily, column, value);
        } catch (Exception ex) {
            logger.error("插入数据到HBase表{}异常!异常信息->{}", tableName, ex);
        }
        /**
         * shell 结果
         * hbase(main):007:0> scan 'quick-hbase-table'
         * ROW                                          COLUMN+CELL
         *  1                                           column=hbase:action, timestamp=1563616496366, value=create table
         *  1                                           column=hbase:time, timestamp=1563616496379, value=2019\xE5\xB9\xB407\xE6\x9C\x8820\xE6\x97\xA517:52:53
         *  1                                           column=hbase:user, timestamp=1563616496384, value=admin
         *  1                                           column=quick:feel, timestamp=1563616496362, value=better
         *  1                                           column=quick:speed, timestamp=1563616496353, value=1km/h
         * 1 row(s)
         */

        return ResultUtil.success();
    }

    /**
     * 获取全表数据
     *
     * @param tableName
     * @return
     */
    @GetMapping(path = "/scan/{tableName}")
    @ApiOperation("获取全表数据")
    public Result<Map<String, Object>> scanTable(@PathVariable String tableName) {
        Map<String, Object> dataMap = new HashMap<>();
        try {
            //扫描表
            String value = hBaseService.scanAllRecord(tableName);
            logger.info("获取到HBase->表{}的内容：\n{}", tableName, value);
            dataMap.put(tableName, value);
        } catch (Exception ex) {
            logger.error("获取HBase表{}的内容异常!异常信息->{}", tableName, ex);
        }

        return ResultUtil.success(dataMap);
    }

    /**
     * 获取指定行数据
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws Exception
     */
    @GetMapping(path = "/selectRow/{tableName}/{rowKey}")
    @ApiOperation("获取指定行数据")
    public Result<String> selectRow(@PathVariable String tableName, @PathVariable String rowKey) throws Exception {
        String value = hBaseService.selectRow(tableName, rowKey);
        logger.info("获取到HBase->表{} ,行{}的内容：\n{}", tableName, rowKey, value);
        return ResultUtil.success(value);
    }

    /**
     * 获取指定版本的列数据记录
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param maxVersions
     * @return
     * @throws Exception
     */
    @GetMapping(path = "/selectColumn")
    @ApiOperation("获取指定版本的列数据记录")
    public Result<String> selectValue(@RequestParam String tableName, @RequestParam String rowKey, @RequestParam String columnFamily,
                                      @RequestParam String column, @RequestParam int maxVersions) throws Exception {
        String value = hBaseService.selectValue(tableName, rowKey, columnFamily, column, maxVersions);
        return ResultUtil.success(value);
    }

    /**
     * 删除数据行
     *
     * @return
     */
    @DeleteMapping(path = "/delete")
    @ApiOperation("删除数据行")
    public Result<String> deleteRow(@RequestParam(value = "tableName") String tableName, String row) {
        try {
            //删除表行
            hBaseService.deleteRow(tableName, row);
        } catch (Exception ex) {
            logger.error("获取HBase表{}的内容异常!异常信息->{}", tableName, ex);
            return ResultUtil.failure(ex.getMessage(), tableName);
        }

        return ResultUtil.success(tableName);
    }
}
