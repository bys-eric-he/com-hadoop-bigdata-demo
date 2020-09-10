package com.hadoop.hive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SpringBootHadoopHiveApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHadoopHiveApplication.class, args);
    }
}
