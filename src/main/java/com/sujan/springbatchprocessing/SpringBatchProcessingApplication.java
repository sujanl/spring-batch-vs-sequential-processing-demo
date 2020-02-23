package com.sujan.springbatchprocessing;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchProcessingApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchProcessingApplication.class, args);
    }

}
