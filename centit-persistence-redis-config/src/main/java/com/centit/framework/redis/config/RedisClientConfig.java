package com.centit.framework.redis.config;

import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.security.SecurityOptUtils;
import io.lettuce.core.RedisClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import javax.annotation.Resource;

/**
 * Created by codefan on 2017/6/14.
 */
public class RedisClientConfig implements EnvironmentAware {

    private Logger logger = LoggerFactory.getLogger(RedisClientConfig.class);

    protected Environment env;

    @Resource
    @Override
    public void setEnvironment(Environment environment) {
        if (environment != null) {
            this.env = environment;
        }
    }

    @Bean
    public RedisClient redisClient() {
        String host = env.getProperty("redis.default.host");
        Integer port = NumberBaseOpt.castObjectToInteger(env.getProperty("redis.default.port"), 6379);
        String password = env.getProperty("redis.default.password");
        Integer database =  NumberBaseOpt.castObjectToInteger(env.getProperty("redis.default.database"), 0);
        // redis:[password@]host[:port][/database]
        StringBuilder redisUri = new StringBuilder("redis://");
        if (StringUtils.isNotBlank(password)) {
            redisUri.append(SecurityOptUtils.decodeSecurityString(password)).append("@");
        }
        redisUri.append(host).append(":").append(port).append("/").append(database);
        return RedisClient.create(redisUri.toString());
    }

}
