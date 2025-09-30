package com.dobest.sync.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class RedisConfig {

    @Value("${redis1.hosts}")
    private String sourceHosts;

    @Value("${redis2.hosts}")
    private String targetHosts;
}
