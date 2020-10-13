package com.hadoop.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

@EnableJpaAuditing
@SpringBootApplication
public class SpringBootHadoopWebApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringBootHadoopWebApplication.class, args);
    }
}
