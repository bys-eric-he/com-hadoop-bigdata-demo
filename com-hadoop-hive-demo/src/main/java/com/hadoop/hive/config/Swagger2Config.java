package com.hadoop.hive.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.time.LocalDateTime;
import java.util.Date;

@Configuration
@EnableSwagger2
public class Swagger2Config {

    /**
     * Swagger组件注册
     */
    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.withClassAnnotation(RestController.class))
                .paths(PathSelectors.any())
                .build()
                .directModelSubstitute(LocalDateTime.class, Date.class);
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("犇犇大数据仓库1.0")
                .description("基于Hive存储的数据仓库,实现数据分层,将数据从来源端经过抽取（extract）、转换（transform）、加载（load）至目的端的过程。")
                .termsOfServiceUrl("http://localhost:8089/swagger-ui.html")
                .contact(new Contact("何涌", "", "heyong@9fstock.com"))
                .version("1.0")
                .build();
    }
}
