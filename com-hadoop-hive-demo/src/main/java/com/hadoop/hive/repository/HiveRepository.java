package com.hadoop.hive.repository;

import com.hadoop.hive.annotation.LogAspect;
import com.hadoop.hive.entity.Employee;
import com.hadoop.hive.entity.EmployeeComplexStructure;
import com.hadoop.hive.entity.Student;
import com.hadoop.hive.entity.StudentHobby;
import com.hadoop.hive.entity.database.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Hive 数据仓库持久化层
 */
@Repository
public class HiveRepository extends HiveBaseJDBCTemplate {

    private final static Logger logger = LoggerFactory.getLogger(HiveRepository.class);

    /**
     * 获取hive数据库数据信息
     * SQL指令必须要有列，不可以select * from .....，否则BeanPropertyRowMapper无法映射到类对象中。
     *
     * @param sql HiveQL select id,name,score,age from student
     * @return
     */
    @LogAspect(value = "getLimitOne")
    public Student getLimitOne(String sql) {
        logger.info("HiveQL-->{}", sql);
        //jdbcTemplate.queryForObject(sql, requiredType) 中的requiredType应该为基础类型，和String类型.
        //return this.getJdbcTemplate().queryForObject(sql, Student.class);

        //如果想查真正的object应该为
        List<Student> studentList = this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(Student.class));
        if (studentList.size() > 0) {
            return studentList.get(0);
        }
        return null;
    }

    /**
     * 获取所以数据信息
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "queryForList")
    public List<Map<String, Object>> queryForList(String sql) {
        return this.getJdbcTemplate().queryForList(sql);
    }

    /**
     * 获取数据列表
     *
     * @param sql SQL指令必须要有列，且列名必须和对象中的属性保持一致，不可以select * from .....，否则BeanPropertyRowMapper无法映射到类对象中。
     * @return
     */
    @LogAspect(value = "getListForObject")
    public List<Student> getListForObject(String sql) {
        //jdbcTemplate.queryForList(sql, requiredType) 中的requiredType应该为基础类型，和String类型.
        //return this.getJdbcTemplate().queryForList(sql, Student.class);

        //如果想查真正的object应该为
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(Student.class));
    }

    /**
     * 获取学生爱好列表
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getListStudentHobby")
    public List<StudentHobby> getListStudentHobby(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(StudentHobby.class));
    }

    /**
     * 获取员工列表
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getListEmployee")
    public List<Employee> getListEmployee(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(Employee.class));
    }

    /**
     * 获取复杂员工列表
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getListEmployeeComplexStructure")
    public List<EmployeeComplexStructure> getListEmployeeComplexStructure(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(EmployeeComplexStructure.class));
    }

    /**
     * 获取指定条件复杂员工列表
     *
     * @param sql
     * @param name
     * @param city
     * @return
     */
    @LogAspect(value = "getListEmployeeComplexStructureByParam")
    public List<EmployeeComplexStructure> getListEmployeeComplexStructureByParam(String sql, String name, String city) {

        RowMapper<EmployeeComplexStructure> rm = BeanPropertyRowMapper.newInstance(EmployeeComplexStructure.class);
        List<Object> queryList = new ArrayList<>();
        if (null != name && !name.equals("")) {
            sql += " and name like ? ";
            queryList.add("%" + name + "%");
        }
        if (null != city && !city.equals("")) {
            sql += " and address.city like ? ";
            queryList.add("%" + city + "%");
        }
        // 使用动态参数agrs[]查询时，会抛DruidPooledPreparedStatement  : getMaxFieldSize error
        // java.sql.SQLFeatureNotSupportedException: Method not supported 异常
        // 如果直接将参数拼接到 sql字符串中则不会抛此异常
        return this.getJdbcTemplate().query(sql, queryList.toArray(), rm);
    }

    /**
     * 列举当前Hive库中的所有数据表
     */
    public List<String> listAllTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        Statement statement = this.getJdbcDataSource().getConnection().createStatement();
        String sql = "show tables";
        logger.info("--->Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            tables.add(res.getString(1));
        }
        return tables;
    }

    /**
     * 创建新表
     *
     * @param sql
     * @return
     */
    public String createTable(String sql) {
        logger.info("-->Running: " + sql);
        String result = "Create table successfully...";
        try {
            this.getJdbcTemplate().execute(sql);
        } catch (DataAccessException dae) {
            result = "********Create table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;

    }

    /**
     * 将Hive服务器本地文档中的数据加载到Hive表中
     *
     * @param filePath  /home/hive_data/user_sample.txt
     * @param tableName user_sample"
     * @return
     */
    public String loadIntoTable(String filePath, String tableName) {
        String result = "Load data into table successfully...";
        try {
            String sql = "load data local inpath '" + filePath + "' into table '" + tableName + "'";
            this.getJdbcTemplate().execute(sql);
        } catch (DataAccessException dae) {
            result = "Load data into table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    /**
     * 向Hive表中添加数据
     *
     * @param sql
     * @return
     */
    public String insertIntoTable(String sql) {
        String result = "Insert into table successfully...";
        try {
            this.getJdbcTemplate().execute(sql);
        } catch (DataAccessException dae) {
            result = "Insert into table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    /**
     * 删除表
     *
     * @param tableName
     * @return
     */
    public String delete(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        String result = "Drop table successfully...";
        logger.info("-->Running: " + sql);
        try {
            this.getJdbcTemplate().execute(sql);
        } catch (DataAccessException dae) {
            result = "Drop table encounter an error: " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    /**
     * 查询Hive库中的某张数据表字段信息
     *
     * @param tableName
     * @return
     * @throws SQLException
     */
    public List<String> describeTable(String tableName) throws SQLException {
        List<String> list = new ArrayList<>();
        Statement statement = this.getJdbcDataSource().getConnection().createStatement();
        String sql = "describe " + tableName;
        logger.info("-->Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            list.add(res.getString(1));
        }
        return list;
    }

    /**
     * 获取表结构详细信息
     *
     * @param tableName 表名
     * @return
     */
    public List<TableInfo> describeTableInfo(String tableName) throws SQLException {
        List<TableInfo> tableInfoList = new ArrayList<>();

        Statement statement = this.getJdbcDataSource().getConnection().createStatement();
        String sql = "describe " + tableName;
        logger.info("-->Running: " + sql);
        ResultSet res = statement.executeQuery(sql);

        while (res.next()) {
            TableInfo tableInfo = new TableInfo();
            tableInfo.setColumnName(res.getString(1));
            tableInfo.setColumnType(res.getString(2));
            tableInfo.setColumnComment(res.getString(3));
            tableInfoList.add(tableInfo);
        }

        return tableInfoList;
    }

    /**
     * 查询指定tableName表中的数据
     *
     * @param tableName 表名
     * @return
     * @throws SQLException
     */
    public List<String> selectFromTable(String tableName) throws SQLException {
        Statement statement = this.getJdbcDataSource().getConnection().createStatement();
        String sql = "select * from " + tableName;
        logger.info("-->Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        List<String> list = new ArrayList<>();
        int count = res.getMetaData().getColumnCount();

        while (res.next()) {
            StringBuilder str = new StringBuilder();
            for (int i = 1; i < count; i++) {
                str.append(res.getString(i)).append(" ");
            }
            str.append(res.getString(count));
            logger.info(str.toString());
            list.add(str.toString());
        }
        return list;
    }
}
