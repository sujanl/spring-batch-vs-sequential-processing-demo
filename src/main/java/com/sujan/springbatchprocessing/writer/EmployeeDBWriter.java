package com.sujan.springbatchprocessing.writer;

import com.sujan.springbatchprocessing.entity.Employee;
import com.sujan.springbatchprocessing.repo.EmployeeRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EmployeeDBWriter implements ItemWriter<Employee> {

    private EmployeeRepository employeeRepository;

    @Autowired
    public EmployeeDBWriter(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    @Override
    public void write(List<? extends Employee> employees) throws Exception {
        employeeRepository.saveAll(employees);
//            System.out.println("inside writer " + employees);
    }
}