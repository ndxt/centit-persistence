package com.centit.framework.redis.config;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.support.spring.data.redis.GenericFastJsonRedisSerializer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Created by zou_wy on 2017/6/14.
 */
@Configuration
public class RedisPersistenceConfig {
    private Logger logger = LoggerFactory.getLogger(RedisPersistenceConfig.class);

    @Value("${redis.default.host:}")
    private String host;

    @Value("${redis.default.port:6379}")
    private Integer port;

    @Value("${redis.default.password:}")
    private String password;

    @Value("${redis.default.database:0}")
    private Integer database;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration configuration =
            new RedisStandaloneConfiguration(host,port);
        logger.debug("Redis Session服务器URL："+host+":"+port+"/"+database);
        System.out.println("Redis Session服务器URL："+host+":"+port+"/"+database);
        configuration.setDatabase(database);
        if(StringUtils.isNotBlank(password)){
            configuration.setPassword(RedisPassword.of(password));
        }
        return new LettuceConnectionFactory(configuration);
    }

    /**
     * @param redisConnectionFactory 这个是 framework-session-redis中的bean耦合
     * @return RedisTemplate bean
     */
    @Bean("objectRedisTemplate")
    public RedisTemplate<String, JSONObject> objectRedisTemplate(@Autowired @Qualifier("redisConnectionFactory")
                                                               RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, JSONObject> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        GenericFastJsonRedisSerializer serializer = new GenericFastJsonRedisSerializer();
        template.setValueSerializer(serializer);
        // 使用StringRedisSerializer来序列化和反序列化redis的key值
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

}
