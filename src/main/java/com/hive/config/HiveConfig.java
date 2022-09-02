package com.hive.config;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.Data;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * HiveConfig
 *
 * @Author: wangxinbiao
 * @DATE: 2022/9/2 17:25
 **/

@Configuration
@ConfigurationProperties(prefix = "hive")
@Data
public class HiveConfig {

    private String url;
    private String user;
    private String password;
    private String driverClassName;
    private int initialSize;
    private int minIdle;
    private int maxActive;
    private int maxWait;
    private int timeBetweenEvictionRunsMillis;
    private int minEvictableIdleTimeMillis;
    private String validationQuery;
    private boolean testWhileIdle;
    private boolean testOnBorrow;
    private boolean testOnReturn;
    private boolean poolPreparedStatements;
    private int maxPoolPreparedStatementPerConnectionSize;
    private int connectionErrorRetryAttempts;
    private boolean breakAfterAcquireFailure;

    @Bean(name = "hiveDruidDataSource")
    @Qualifier("hiveDruidDataSource")
    public DataSource dataSource() {
        DruidDataSource datasource = new DruidDataSource();
        datasource.setUrl(url);
        datasource.setUsername(user);
        datasource.setPassword(password);
        datasource.setDriverClassName(driverClassName);

        // pool configuration
        datasource.setInitialSize(initialSize);
        datasource.setMinIdle(minIdle);
        datasource.setMaxActive(maxActive);
        datasource.setMaxWait(maxWait);
        datasource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        datasource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        datasource.setValidationQuery(validationQuery);
        datasource.setTestWhileIdle(testWhileIdle);
        datasource.setTestOnBorrow(testOnBorrow);
        datasource.setTestOnReturn(testOnReturn);
        datasource.setPoolPreparedStatements(poolPreparedStatements);
        datasource.setMaxPoolPreparedStatementPerConnectionSize(
                maxPoolPreparedStatementPerConnectionSize);
        return datasource;
    }

    @Bean(name = "hiveDruidTemplate")
    public JdbcTemplate hiveDruidTemplate(@Qualifier("hiveDruidDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
