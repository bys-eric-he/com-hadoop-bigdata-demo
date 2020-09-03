package com.hadoop.hbase.service.impl;

import com.hadoop.hbase.exception.CustomException;
import com.hadoop.hbase.exception.EnumerationException;
import com.hadoop.hbase.repository.HBaseRepository;
import com.hadoop.hbase.service.HBaseService;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

@Service
public class HBaseServiceImpl implements HBaseService {

    private final Logger logger = LoggerFactory.getLogger(HBaseServiceImpl.class);

    /**
     * 检查表是否已经存在
     *
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    @Override
    public boolean tableExists(String tableName) throws IOException {
        TableName[] tableNames = HBaseRepository.getInstance().getAdmin().listTableNames();
        if (tableNames != null && tableNames.length > 0) {
            for (TableName name : tableNames) {
                if (tableName.equals(name.getNameAsString())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 创建表
     *
     * @param tableName      表名
     * @param columnFamilies 列族（数组）
     */
    @Override
    public void createTable(String tableName, String[] columnFamilies) throws Exception {
        TableName name = TableName.valueOf(tableName);
        boolean isExists = this.tableExists(tableName);
        if (isExists) {
            logger.error("创建HBase 表失败! 表 {} 已经存在, 且已经执行删除操作, 请重新创建!", name);
            throw new TableExistsException(tableName + "is exists!");
        }
        try {
            TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(name);
            List<ColumnFamilyDescriptor> columnFamilyList = new ArrayList<>();
            for (String columnFamily : columnFamilies) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                        .newBuilder(columnFamily.getBytes()).build();
                columnFamilyList.add(columnFamilyDescriptor);
            }
            descriptorBuilder.setColumnFamilies(columnFamilyList);
            TableDescriptor tableDescriptor = descriptorBuilder.build();
            HBaseRepository.getInstance().getAdmin().createTable(tableDescriptor);
            logger.info("创建HBase 表成功! 表->{} ,列族->{} !", name, StringUtils.join(columnFamilies, ','));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new CustomException(EnumerationException.PARAMETER_ERROR);
        }
    }

    /**
     * 创建表
     *
     * @param tableName      表名
     * @param columnFamilies 列族（数组）
     * @param isExistsRemove 如果表存在是否删除
     */
    @Override
    public void createTable(String tableName, String[] columnFamilies, Boolean isExistsRemove) throws IOException {
        TableName name = TableName.valueOf(tableName);
        //如果存在则删除
        if (HBaseRepository.getInstance().getAdmin().tableExists(name)) {
            if (isExistsRemove) {
                HBaseRepository.getInstance().getAdmin().disableTable(name);
                HBaseRepository.getInstance().getAdmin().deleteTable(name);
                logger.error("创建HBase 表失败! 表 {} 已经存在, 且已经执行删除操作, 请重新创建!", name);
            } else {
                logger.error("创建HBase 表失败! 表 {} 已经存在!", name);
            }

        } else {
            TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(name);
            List<ColumnFamilyDescriptor> columnFamilyList = new ArrayList<>();
            for (String columnFamily : columnFamilies) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                        .newBuilder(columnFamily.getBytes()).build();
                columnFamilyList.add(columnFamilyDescriptor);
            }
            descriptorBuilder.setColumnFamilies(columnFamilyList);
            TableDescriptor tableDescriptor = descriptorBuilder.build();
            HBaseRepository.getInstance().getAdmin().createTable(tableDescriptor);
        }
    }

    /**
     * 插入记录（单行单列族-多列多值）
     *
     * @param tableName      表名
     * @param row            行名
     * @param columnFamilies 列族名
     * @param columns        列名（数组）
     * @param values         值（数组）（且需要和列一一对应）
     */
    @Override
    public void insertRecords(String tableName, String row, String columnFamilies, String[] columns, String[] values) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamilies), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
            table.put(put);
        }
    }

    /**
     * 插入记录（单行单列族-单列单值）
     *
     * @param tableName    表名
     * @param row          行名
     * @param columnFamily 列族名
     * @param column       列名
     * @param value        值
     */
    @Override
    public void insertOneRecord(String tableName, String row, String columnFamily, String column, String value) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        logger.info("插入HBase 数据成功! 表->{} 行->{} 列族->{} 列->{} 值->{}!", tableName, row, columnFamily, column, value);
    }

    /**
     * 删除一行记录
     *
     * @param tableName 表名
     * @param rowKey    行名
     */
    @Override
    public void deleteRow(String tableName, String rowKey) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Delete d = new Delete(rowKey.getBytes());
        table.delete(d);
        logger.info("删除HBase 数据成功! 表->{} 行->{} ", tableName, rowKey);
    }

    /**
     * 删除单行单列族记录
     *
     * @param tableName    表名
     * @param rowKey       行名
     * @param columnFamily 列族名
     */
    @Override
    public void deleteColumnFamily(String tableName, String rowKey, String columnFamily) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Delete d = new Delete(rowKey.getBytes()).addFamily(Bytes.toBytes(columnFamily));
        table.delete(d);
        logger.info("删除HBase 单行单列族记录数据成功! 表->{} 行->{} 列族->{}", tableName, rowKey, columnFamily);
    }

    /**
     * 删除单行单列族单列记录
     *
     * @param tableName    表名
     * @param rowKey       行名
     * @param columnFamily 列族名
     * @param column       列名
     */
    @Override
    public void deleteColumn(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Delete d = new Delete(rowKey.getBytes()).addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(d);
        logger.info("删除HBase 单行单列族单列记录数据成功! 表->{} 行->{} 列族->{} 列->{}", tableName, rowKey, columnFamily, column);
    }

    /**
     * 查找一行记录
     *
     * @param tableName 表名
     * @param rowKey    行名
     */
    @Override
    public String selectRow(String tableName, String rowKey) throws IOException {
        StringBuilder record = new StringBuilder();
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Get g = new Get(rowKey.getBytes());
        Result rs = table.get(g);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = rs.getMap();
        for (Cell cell : rs.rawCells()) {
            String str = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + "\t" +
                    Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "\t" +
                    Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "\t" +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) + "\n";
            // 可以通过反射封装成对象(列名和Java属性保持一致)
            record.append(str);
        }
        logger.info("HBase 单查找一行记录据成功! 表->{} 行->{} 数据->{}", tableName, rowKey, record.toString());
        return record.toString();
    }

    /**
     * 查找单行单列族单列记录
     *
     * @param tableName    表名
     * @param rowKey       行名
     * @param columnFamily 列族名
     * @param column       列名
     * @return
     */
    @Override
    public String selectValue(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Get g = new Get(rowKey.getBytes());
        g.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Result rs = table.get(g);
        String result = Bytes.toString(rs.value());
        logger.info("HBase 查找单行单列族单列记录据成功! 表->{} 行->{} 列族->{} 列->{} 数据->{}", tableName, rowKey, columnFamily, column, result);
        return result;
    }

    /**
     * 查找单行单列族单列记录
     *
     * @param tableName    表名
     * @param rowKey       行名
     * @param columnFamily 列族名
     * @param column       列名
     * @param maxVersions  获取多少个版本的数据
     * @return
     */
    @Override
    public String selectValue(String tableName, String rowKey, String columnFamily, String column, int maxVersions) throws Exception {
        if (maxVersions <= 0) {
            throw new CustomException(EnumerationException.PARAMETER_ERROR);
        }
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Get g = new Get(rowKey.getBytes());
        g.readVersions(maxVersions);
        g.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Result rs = table.get(g);
        String result = Bytes.toString(rs.value());
        logger.info("HBase 查找单行单列族单列记录据成功! 表->{} 行->{} 列族->{} 列->{} 数据->{} 版本->{}",
                tableName, rowKey, columnFamily, column, result, maxVersions);
        return result;
    }

    /**
     * 查询表中所有行（Scan方式）
     *
     * @param tableName 表名
     * @return
     */
    @Override
    public String scanAllRecord(String tableName) throws IOException {
        StringBuilder record = new StringBuilder();
        TableName name = TableName.valueOf(tableName);
        Table table = HBaseRepository.getInstance().getConnection().getTable(name);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    String value = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + "\t" +
                            Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "\t" +
                            Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "\t" +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) + "\n";
                    record.append(value);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return record.toString();
    }

    /**
     * 根据rowkey关键字查询报告记录
     *
     * @param tablename
     * @param rowKeyword
     * @return
     */
    @Override
    public List<Object> scanReportDataByRowKeyword(String tablename, String rowKeyword) throws IOException {
        ArrayList<Object> list = new ArrayList<Object>();

        Table table = HBaseRepository.getInstance().getConnection().getTable(TableName.valueOf(tablename));
        Scan scan = new Scan();
        if (!StringUtils.isEmpty(rowKeyword)) {
            //添加行键过滤器，根据关键字匹配
            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKeyword));
            scan.setFilter(rowFilter);
        }

        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    String value = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + "\t" +
                            Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "\t" +
                            Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "\t" +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) + "\n";
                    list.add(value);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return list;
    }

    /**
     * 根据rowkey关键字和时间戳范围查询报告记录
     *
     * @param tableName  表名
     * @param rowKeyword rowKey关键字
     * @param minStamp   最小时间
     * @param maxStamp   最大时间
     * @return
     */
    @Override
    public List<Object> scanReportDataByRowKeywordTimestamp(String tableName, String rowKeyword, Long minStamp, Long maxStamp) throws IOException {
        ArrayList<Object> list = new ArrayList<Object>();

        Table table = HBaseRepository.getInstance().getConnection().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //添加scan的时间范围
        scan.setTimeRange(minStamp, maxStamp);
        if (!StringUtils.isEmpty(rowKeyword)) {
            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKeyword));
            scan.setFilter(rowFilter);
        }

        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    String value = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + "\t" +
                            Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "\t" +
                            Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "\t" +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) + "\n";
                    list.add(value);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return list;
    }

    /**
     * 删除表操作
     *
     * @param tableName 表名
     */
    @Override
    public void deleteTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (HBaseRepository.getInstance().getAdmin().tableExists(name)) {
            HBaseRepository.getInstance().getAdmin().disableTable(name);
            HBaseRepository.getInstance().getAdmin().deleteTable(name);
        }
    }
}
