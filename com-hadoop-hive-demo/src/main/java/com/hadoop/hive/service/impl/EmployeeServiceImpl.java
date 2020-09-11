package com.hadoop.hive.service.impl;

import com.hadoop.hive.entity.employee.Employee;
import com.hadoop.hive.entity.employee.EmployeeComplexStructure;
import com.hadoop.hive.repository.EmployeeRepository;
import com.hadoop.hive.service.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EmployeeServiceImpl implements EmployeeService {

    @Autowired
    private EmployeeRepository employeeRepository;

    /**
     * 获取员工记录
     *
     * @return
     */
    @Override
    public List<Employee> getListEmployee() {
        String sql = "select id,info from employee";
        return employeeRepository.getListEmployee(sql);
    }

    /**
     * 获取复杂员工记录
     *
     * @return
     */
    @Override
    public List<EmployeeComplexStructure> getListEmployeeComplexStructure() {
        String sql = "select name,sa1ary,subordinates,deductions,address from employee_complex_structure";
        return employeeRepository.getListEmployeeComplexStructure(sql);
    }

    /**
     * 查询指定条件复杂员工记录
     *
     * @param name
     * @param city
     * @return
     */
    @Override
    public List<EmployeeComplexStructure> getListEmployeeComplexStructureByParam(String name, String city) {
        String sql = "select name,sa1ary,subordinates,deductions,address from employee_complex_structure where 1=1 ";
        return employeeRepository.getListEmployeeComplexStructureByParam(sql, name, city);
    }
}
