package com.dobest.sync.util;

import com.github.luben.zstd.Zstd;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
@Component
public class RedisCompressionUtil {

    @Autowired
    private RedisClientUtil redisClientUtil;

    RedissonClient redisClient;
    @PostConstruct
    public void initRedisClient(){
        redisClient =redisClientUtil.getRedissonTarget();

    }

    public void setWithZstd(String key,String value) {
        try {
            byte[] compressed = Zstd.compress(value.getBytes(StandardCharsets.UTF_8), 3);
            redisClient.getBucket(key).set(compressed);
        } catch (Exception e) {
            log.error("compression redis key fail ", key);
        }
    }

    /**
     * 批量压缩
     * @param values
     */
    public void setBatchWithZstd(Map<String,String> values){
        log.info("execute size {} , time {}",values.size(),System.currentTimeMillis());
        RBatch batch = redisClient.createBatch();
        values.forEach((key, value) -> {
            byte[] compressed = Zstd.compress(
                    value.getBytes(StandardCharsets.UTF_8),
                    3  // 压缩级别
            );
            batch.getBucket(key).setAsync(compressed); // 异步批量写入
        });
        batch.execute();
        log.info("execute ok ,time {} ",System.currentTimeMillis());
    }

    public void setKey(String key,String value){
        redisClient.getBucket(key).set(value);
    }
    public String get(String key){
        if(redisClient.getBucket(key).isExists()){
            return (String) redisClient.getBucket(key).get();
        }
        return null;



    }
    public String getWithUnZstd(String key) {
        try {
            byte[] compressed = (byte[]) redisClient.getBucket(key).get();
            String original =
                    new String(
                            Zstd.decompress(compressed, compressed.length * 10), // 预估解压大小
                            StandardCharsets.UTF_8);
            return original;
        } catch (Exception e) {
            log.error("un compress zstd redis fail ,key  " + key);
            return null;
        }
    }



}
