package com.sujan.springbatchprocessing.controller;

import com.opencsv.CSVReader;
import com.sujan.springbatchprocessing.entity.Employee;
import com.sujan.springbatchprocessing.repo.EmployeeRepository;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/job")
public class JobController {
    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job csvToDbJob;

    @Autowired
    EmployeeRepository employeeRepository;

    @GetMapping("/batch")
    public String loadCSVTODBJob() throws JobParametersInvalidException,
            JobExecutionAlreadyRunningException,
            JobRestartException,
            JobInstanceAlreadyCompleteException {

        Map<String, JobParameter> maps = new HashMap<>();
        maps.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(maps);
        JobExecution jobExecution = jobLauncher.run(csvToDbJob, parameters);
        return String.format("Job "+jobExecution.getJobInstance()+" submitted successfully."+jobExecution.getStatus().toString());
    }

    @GetMapping("/sequential")
    public String csvToDbSimply(){
        long startTime = System.nanoTime();
        String csvFile = "src/main/resources/employees.csv";
        List<Employee> employeeList = new ArrayList<>();
        try {
            CSVReader reader = new CSVReader(new FileReader(csvFile));
            String[] line;
            while ((line = reader.readNext()) != null) {
                Employee employee = new Employee();
                employee.setFirstName(line[0]);
                employee.setLastName(line[1]);
                employee.setEmail(line[2]);
                employee.setAge(Integer.parseInt(line[3]));
                System.out.println("Employee==> "+employee.getFirstName()
                        +", "+employee.getLastName()
                        +", "+employee.getEmail()
                        +", "+employee.getAge());
                employeeList.add(employee);
                System.out.println(employeeList.size());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Saving to database....");
        employeeRepository.saveAll(employeeList);
        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("Total elapsed time in nanoseconds: " + elapsedTime);
        return "Total elapsed time: " + elapsedTime/1_000_000_000+"sec\n SIze: "+employeeList.size();
    }
}
