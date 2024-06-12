package com.centit.framework.redis.config;

import com.centit.support.security.SecurityOptUtils;
import io.lettuce.core.RedisClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by codefan on 2017/6/14.
 */
@Configuration
public class RedisClientConfig {
    private Logger logger = LoggerFactory.getLogger(RedisClientConfig.class);

    @Value("${redis.default.host:}")
    private String host;

    @Value("${redis.default.port:6379}")
    private Integer port;

    @Value("${redis.default.password:}")
    private String password;

    @Value("${redis.default.database:0}")
    private Integer database;

    @Bean
    public RedisClient redisClient() {
        // redis:[password@]host[:port][/database]
        StringBuilder redisUri = new StringBuilder("redis://");
        if (StringUtils.isNotBlank(password)) {
            redisUri.append(SecurityOptUtils.decodeSecurityString(password)).append("@");
        }
        redisUri.append(host).append(":").append(port).append("/").append(database);
        return RedisClient.create(redisUri.toString());
    }

}
