package com.hadoop.web.config;


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
                .title("犇犇大数据仓库可视化数据接口1.0")
                .description("查询Hive计算结果,数据由Hive ADS层通过Sqoop同步到MySQL相同表结构中存储,以便可视化查询和显示。")
                .termsOfServiceUrl("http://localhost:8080/swagger-ui.html")
                .contact(new Contact("何涌", "", "heyong@9fstock.com"))
                .version("1.0")
                .build();
    }
}