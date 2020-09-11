package com.hadoop.hive.entity.car;

import lombok.Data;

/**
 * 车辆销售数据
 */
@Data
public class Car {
    /**
     * 省份
     */
    private String province;
    /**
     * 月
     */
    private int month;
    /**
     * 市
     */
    private String city;
    /**
     * 区县
     */
    private String county;
    /**
     * 年
     */
    private int year;
    /**
     * 车辆型号
     */
    private String carype;
    /**
     * 制造商
     */
    private String productor;
    /**
     * 品牌
     */
    private String brand;
    /**
     * 车辆类型
     */
    private String mold;
    /**
     * 所有权
     */
    private String owner;
    /**
     * 使用性质
     */
    private String nature;
    /**
     * 数量
     */
    private int number;
    /**
     * 发动机型号
     */
    private String ftype;
    /**
     * 排量
     */
    private int outv;
    /**
     * 功率
     */
    private double power;
    /**
     * 燃料种类
     */
    private String fuel;
    /**
     * 车长
     */
    private int length;
    /**
     * 车宽
     */
    private int width;
    /**
     * 车高
     */
    private int height;
    /**
     * 厢长
     */
    private int xlength;
    /**
     * 厢宽
     */
    private int xwidth;
    /**
     * 厢高
     */
    private int xheight;
    /**
     * 轴数
     */
    private int count;
    /**
     * 轴距
     */
    private int base;
    /**
     * 前轮距
     */
    private int front;
    /**
     * 轮胎规格
     */
    private String norm;
    /**
     * 轮胎数
     */
    private int tnumber;
    /**
     * 总质量
     */
    private int total;
    /**
     * 整备质量
     */
    private int curb;
    /**
     * 核定载质量
     */
    private int hcurb;
    /**
     * 核定载客
     */
    private String passenger;
    /**
     * 准牵引质量
     */
    private int zhcurb;
    /**
     * 底盘企业
     */
    private String business;
    /**
     * 底盘品牌
     */
    private String dtype;
    /**
     * 底盘型号
     */
    private String fmold;
    /**
     * 发动机企业
     */
    private String fbusiness;
    /**
     * 车辆名称
     */
    private String name;
    /**
     * 年龄
     */
    private int age;
    /**
     * 性别
     */
    private String sex;
}
