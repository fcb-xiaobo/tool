package com.dobest.sync.util;

import com.alibaba.fastjson.JSONObject;
import com.dobest.sync.config.RedisConfig;
import com.github.luben.zstd.Zstd;
import org.redisson.Redisson;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class RedisClientUtil {

    private static final String PREFIX = "redis://";

    private static final String KEY_PREFIX_OLD = "c#";

    private static final int SCAN_BATCH_SIZE = 1000; // 每次SCAN获取数量
    private static final int PIPELINE_BATCH_SIZE = 500; // 管道批量操作数量

    @Autowired
    private RedisConfig redisSourceConfig;

    @Autowired
    private RedisConfig redisTargetConfig;
    RedissonClient redissonSource;

    RedissonClient redissonTarget;


    @PostConstruct
    public void initSourceRedisClient() {
        Config config = new Config();
        String[] hosts =
                Arrays.stream(redisSourceConfig.getSourceHosts().split(","))
                        .map(PREFIX::concat)
                        .toArray(String[]::new);
        config.useClusterServers().addNodeAddress(hosts).setTimeout(10000);
        config.setCodec(new StringCodec());
        redissonSource = Redisson.create(config);
    }

    @PostConstruct
    public void initTargetRedisClient() {
        Config config = new Config();
        String[] hosts =
                Arrays.stream(redisTargetConfig.getTargetHosts().split(","))
                        .map(PREFIX::concat)
                        .toArray(String[]::new);
        config.useClusterServers().addNodeAddress(hosts);
        config.setCodec(ByteArrayCodec.INSTANCE);
        redissonTarget = Redisson.create(config);
    }

    public RedissonClient getRedissonTarget() {
        if (redissonTarget != null) {
            return redissonTarget;
        }
        throw new RuntimeException("初始化target redis cluster fail");
    }

    public String getKey(String key){
        return (String) redissonSource.getBucket(key).get();
    }
    /**
     * // 使用SCAN迭代避免KEYS阻塞
     * Iterable<String> keys = redisson.getKeys()
     * .getKeysByPattern(pattern, SCAN_BATCH_SIZE);
     * <p>
     * // 管道批量读取
     * RBatch batch = redisson.createBatch();
     * int count = 0;
     * <p>
     * for (String key : keys) {
     * batch.getBucket(key, StringCodec.INSTANCE).getAsync();
     * if (++count % PIPELINE_BATCH_SIZE == 0) {
     * executeBatch(batch, result);
     * batch = redisson.createBatch();
     * }
     * }
     * <p>
     * // 处理剩余数据
     * if (count % PIPELINE_BATCH_SIZE != 0) {
     * executeBatch(batch, result);
     * }
     * <p>
     * return result;
     */

//    public Map<String, String> getRedisKeyAndValue() {
//        System.out.println("redis source -> "+redisSourceConfig.getSourceHosts());
//        System.out.println("redis target -> "+redisSourceConfig.getTargetHosts());
//
//
//        Map<String, String> result = new LinkedHashMap<>();
//        String pattern = KEY_PREFIX_OLD + "*";
//
//        // 获取所有匹配的key
//        Iterable<String> keys = redissonSource.getKeys()
//                .getKeysByPattern(pattern, SCAN_BATCH_SIZE);
//
//        // 创建批量操作
//        RBatch batch = redissonSource.createBatch();
//        List<String> keyList = new ArrayList<>();
//
//        // 收集key并准备批量查询
//        keys.forEach(key -> {
//            keyList.add(key);
//            batch.getBucket(key).getAsync();
//        });
//
//        // 执行批量查询并获取结果
//        List<Object> values = (List<Object>) batch.execute().getResponses();
//
//        // 将key和value对应起来
////        for (int i = 0; i < keyList.size(); i++) {
////            if (values.get(i) != null) {
////                JSONObject value = JSONObject.parseObject(values.get(i).toString());
////                value.remove("appId");
////                value.remove("attrColValue");
////                String redisKey = keyList.get(i);
////                result.put(redisKey.replace("cube","c"), value.toJSONString());
////            }
////        }
//
//        System.out.println("search redis size -> " +result.size());
//        return result;
//
//        // 此时keys和values按顺序对应
//    }

//    public List<String> scanRedisForBatch(){
//        List<String> result = new ArrayList<>();
//        String pattern = KEY_PREFIX_OLD + "*";
//
//        // 使用SCAN迭代避免KEYS阻塞
//        Iterable<String> keys = redissonSource.getKeys()
//                .getKeysByPattern(pattern, SCAN_BATCH_SIZE);
//
//        // 管道批量读取
//        RBatch batch = redissonSource.createBatch();
//        int count = 0;
//
//        for (String key : keys) {
//            batch.getBucket(key, StringCodec.INSTANCE).getAsync();
//            if (++count % PIPELINE_BATCH_SIZE == 0) {
//                executeBatch(batch, result);
//                batch = redissonSource.createBatch();
//            }
//        }
//
//        // 处理剩余数据
//        if (count % PIPELINE_BATCH_SIZE != 0) {
//            executeBatch(batch, result);
//        }
//
//        return result;
//    }
//
//    private void executeBatch(RBatch batch, List<String> result) {
//        // 获取批量操作的所有响应
//        List<Object> responses = (List<Object>) batch.execute().getResponses();
//        // 处理非空响应
//        responses.stream()
//                .filter(Objects::nonNull)
//                .forEach(obj -> result.add((String) obj));
//
//
//    }

}
