package com.hadoop.web.repository;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@Repository
public class JPARepository {

    /**
     * 容器托管的EntityManager对象
     * 容器托管的EntityManager对象最为简单，编程人员不需要考虑EntityManger的连接，
     * 释放以及复杂的事务问题等等，所有这些都交给容器来完成。
     */
    @PersistenceContext
    private EntityManager entityManager;

    @Bean
    public JPAQueryFactory jpaQueryFactory() {
        return new JPAQueryFactory(entityManager);
    }
}