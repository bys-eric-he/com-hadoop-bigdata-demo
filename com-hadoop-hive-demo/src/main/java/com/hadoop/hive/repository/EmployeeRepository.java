package com.hadoop.hive.repository;

import com.hadoop.hive.annotation.LogAspect;
import com.hadoop.hive.entity.employee.Employee;
import com.hadoop.hive.entity.employee.EmployeeComplexStructure;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
public class EmployeeRepository extends HiveBaseJDBCTemplate {
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
}
