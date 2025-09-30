package com.dobest.sync.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class StarRocksConfig {

    @Value("${sr.jdbc-url}")
    private String srJdbcUrl;

    @Value("${sr.load-url}")
    private String srLoadUrl;

    @Value("${sr.user-name}")
    private String userName;

    @Value("${sr.password}")
    private String password;
}
