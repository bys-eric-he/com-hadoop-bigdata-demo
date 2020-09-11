package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.employee.Employee;
import com.hadoop.hive.entity.employee.EmployeeComplexStructure;
import com.hadoop.hive.service.EmployeeService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "EmployeeController", description = "员工信息统计")
@RestController
@RequestMapping("/api/v1/employee")
public class EmployeeController {

    @Autowired
    private EmployeeService employeeService;

    @ApiOperation("获取员工记录")
    @RequestMapping(path = "/listEmployeeForObject", method = RequestMethod.GET)
    public Result<List<Employee>> getListEmployee() {
        List<Employee> result = employeeService.getListEmployee();
        return ResultUtil.success(result);
    }

    @ApiOperation("获取复杂员工记录")
    @RequestMapping(path = "/listEmployeeComplexStructureForObject", method = RequestMethod.GET)
    public Result<List<EmployeeComplexStructure>> getListEmployeeComplexStructure() {
        List<EmployeeComplexStructure> result = employeeService.getListEmployeeComplexStructure();
        return ResultUtil.success(result);
    }

    @ApiOperation("查询指定条件复杂员工记录")
    @RequestMapping(path = "/listEmployeeComplexStructureForObjectByParam", method = RequestMethod.POST)
    public Result<List<EmployeeComplexStructure>> getListEmployeeComplexStructureByParam(String name, String city) {
        List<EmployeeComplexStructure> result = employeeService.getListEmployeeComplexStructureByParam(name, city);
        return ResultUtil.success(result);
    }
}
