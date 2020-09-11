package com.hadoop.hive.service;

import com.hadoop.hive.entity.employee.Employee;
import com.hadoop.hive.entity.employee.EmployeeComplexStructure;

import java.util.List;

public interface EmployeeService {
    /**
     * 获取员工记录
     *
     * @return
     */
    List<Employee> getListEmployee();

    /**
     * 获取复杂员工记录
     *
     * @return
     */
    List<EmployeeComplexStructure> getListEmployeeComplexStructure();

    /**
     * 查询指定条件复杂员工记录
     *
     * @param name
     * @param city
     * @return
     */
    List<EmployeeComplexStructure> getListEmployeeComplexStructureByParam(String name, String city);
}
