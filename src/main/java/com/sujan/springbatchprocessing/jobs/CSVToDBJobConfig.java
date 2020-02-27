package com.sujan.springbatchprocessing.jobs;

import com.sujan.springbatchprocessing.dto.EmployeeDto;
import com.sujan.springbatchprocessing.entity.Employee;
import com.sujan.springbatchprocessing.processor.EmployeeDtoToEmployeeProcessor;
import com.sujan.springbatchprocessing.writer.EmployeeDBWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

@Configuration
public class CSVToDBJobConfig {

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private EmployeeDtoToEmployeeProcessor employeeDtoToEmployeeProcessor;
    private EmployeeDBWriter employeeDBWriter;

    public CSVToDBJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EmployeeDtoToEmployeeProcessor employeeDtoToEmployeeProcessor, EmployeeDBWriter employeeDBWriter){
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeDtoToEmployeeProcessor = employeeDtoToEmployeeProcessor;
        this.employeeDBWriter = employeeDBWriter;
    }

    @Bean
    public Job csvToDb() {
        return this.jobBuilderFactory.get("CSVToDB")
                .start(csvToDbStep())
                .build();
    }

    @Bean
    public Step csvToDbStep() {
        return this.stepBuilderFactory.get("CSVToDB-step")
                .<EmployeeDto, Employee>chunk(500)
                .reader(employeeReader())
                .processor(employeeDtoToEmployeeProcessor)
                .writer(employeeDBWriter)
//                .faultTolerant().skipPolicy(new SkipPolicy() {
//                    @Override
//                    public boolean shouldSkip(Throwable throwable, int failedCount) throws SkipLimitExceededException {
//                        return (failedCount >= 5) ? false : true;//skip the fault job but if fault job exceed 5 stop job execution
//                    }
//                })
                .taskExecutor(getTaskExecutor())
                .build();
    }

    @Bean
    @StepScope
    public TaskExecutor getTaskExecutor() {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(5);
        return simpleAsyncTaskExecutor;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDto> employeeReader() {
        FlatFileItemReader<EmployeeDto> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("src/main/resources/employees.csv"));
        reader.setLineMapper(lineMapper());
        return reader;
    }

    @Bean
    @StepScope
    public LineMapper<EmployeeDto> lineMapper() {
        DefaultLineMapper<EmployeeDto> defaultLineMapper = new DefaultLineMapper<>();

        defaultLineMapper.setLineTokenizer(getDelimitedLineTokenizer());
        defaultLineMapper.setFieldSetMapper(getFieldSetMapper());

        return defaultLineMapper;
    }

    @Bean
    @StepScope
    public FieldSetMapper<EmployeeDto> getFieldSetMapper() {
        BeanWrapperFieldSetMapper<EmployeeDto> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(EmployeeDto.class);
        return fieldSetMapper;
    }

    @Bean
    @StepScope
    public DelimitedLineTokenizer getDelimitedLineTokenizer(){
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setNames(new String[]{"firstName", "lastName", "email", "age"});

        return lineTokenizer;
    }

}
