package com.hadoop.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SpringBootHadoopSparkApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHadoopSparkApplication.class, args);
    }

}
