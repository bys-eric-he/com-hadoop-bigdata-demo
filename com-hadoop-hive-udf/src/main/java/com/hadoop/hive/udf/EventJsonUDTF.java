package com.hadoop.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

/**
 * 自定义 UDTF 函数（解析事件字段） 安装步骤：
 * 1. 打包该程序,将jar包 com-hadoop-hive-udf-1.0-SNAPSHOT.jar 上传到hadoop 节点目录 ，然后再将该 jar 包上 传到 HDFS 的/user/hive/jars 路径下
 * 2. 在HDFS目录 创建./hadoop fs -mkdir -p /user/hive/jars 目录
 * 3. 将com-hadoop-hive-udf-1.0-SNAPSHOT.jar 上传到HDFS目录 ./hadoop fs -put /home/hive_module/com-hadoop-hive-udf-1.0-SNAPSHOT.jar /user/hive/jars
 * 4. 创建永久函数与开发好的 java class 关联
 *    hive> create function base_analizer as 'com.hadoop.hive.udf.BaseFieldUDF'
 *          using jar 'hdfs://172.12.0.4:9000/user/hive/jars/com-hadoop-hive-udf-1.0-SNAPSHOT.jar';
 *          create function flat_analizer as 'com.hadoop.hive.udf.EventJsonUDTF'
 *          using jar 'hdfs://172.12.0.4:9000/user/hive/jars/com-hadoop-hive-udf-1.0-SNAPSHOT.jar';
 * 5. 注意：如果修改了自定义函数重新生成 jar 包怎么处理？只需要替换 HDFS 路径上的旧 jar 包，然后重启 Hive 客户端即可。
 */
public class EventJsonUDTF extends GenericUDTF {
    //该方法中，我们将指定输出参数的名称和参数类型：
    public StructObjectInspector initialize(StructObjectInspector argOIs) {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * UDTF 函数特点：多行进多行出。
     * 简称，多进多出。
     *
     * @param objects
     * @throws HiveException
     */
    @Override
    public void process(Object[] objects) throws HiveException {
        // 获取传入的 et
        String input = objects[0].toString();
        // 如果传进来的数据为空，直接返回过滤掉该数据
        if (StringUtils.isBlank(input)) {
            return;
        } else {
            try {
                // 获取一共有几个事件（ad/facoriters）
                JSONArray ja = new JSONArray(input);
                if (ja == null) return;
                // 循环遍历每一个事件
                for (int i = 0; i < ja.length(); i++) {
                    String[] result = new String[2];
                    try {
                        // 取出每个的事件名称（ad/facoriters）
                        result[0] = ja.getJSONObject(i).getString("en");
                        // 取出每一个事件整体
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }
                    // 将结果返回
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    // 当没有记录处理的时候该方法会被调用，用来清理代码或者产生额外的输出
    @Override
    public void close() throws HiveException {
    }
}