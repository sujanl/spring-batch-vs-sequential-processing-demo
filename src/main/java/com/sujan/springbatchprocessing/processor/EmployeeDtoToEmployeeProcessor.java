package com.sujan.springbatchprocessing.processor;

import com.sujan.springbatchprocessing.dto.EmployeeDto;
import com.sujan.springbatchprocessing.entity.Employee;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EmployeeDtoToEmployeeProcessor implements ItemProcessor<EmployeeDto, Employee> {
    @Override
    public Employee process(EmployeeDto employeeDTO) throws Exception {
        //can do 'employDto' validation over here and return 'null'
        Employee employee = new Employee();
        employee.setFirstName(employeeDTO.getFirstName());
        employee.setLastName(employeeDTO.getLastName());
        employee.setEmail(employeeDTO.getEmail());
        employee.setAge(employeeDTO.getAge());
//        System.out.println("inside processor " + employee.toString());
        return employee;
    }
}
