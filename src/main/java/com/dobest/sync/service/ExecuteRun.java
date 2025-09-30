package com.dobest.sync.service;

import com.alibaba.fastjson.JSONObject;
import com.dobest.sync.pojo.RedisCacheObj;
import com.dobest.sync.util.RedisClientUtil;
import com.dobest.sync.util.RedisCompressionUtil;
import com.dobest.sync.util.StarRocksJdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;


import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class ExecuteRun implements CommandLineRunner {

    @Autowired
    private RedisClientUtil redisClientUtil;

    @Autowired
    private RedisCompressionUtil redisCompressionUtil;

    @Autowired
    private StarRocksJdbcUtil starRocksJdbcUtil;


    @Override
    public void run(String... args) throws Exception {
        if(args.length != 4){
            System.out.println("param error");
            System.exit(0);
        }
        String db=args[0];
        String table=args[1];
        String appId=args[2];
        int size=Integer.parseInt(args[3]);
        Map<String,String> dataMap=new HashMap<>();
        String printS=String.format("db [ %s ] ,table [ %s ],appid [ %s ] ,size [ %d ]",db,table,appId,size);
        System.out.println(printS);
        String tableinfo=db+"."+table;
        switch (table){
            case "device_dict_meta":{

                starRocksJdbcUtil.streamQuery(tableinfo , size, rs -> {
                    JSONObject json=new JSONObject();
                    String deviceId = rs.getString("device_id");
                    json.put("has_init",rs.getBoolean("has_init"));
                    json.put("first_event",JSONObject.parseObject(rs.getString("first_event")));
                    json.put("first_field",JSONObject.parseObject(rs.getString("first_field")));
                    json.put("event_append",JSONObject.parseObject(rs.getString("event_append")));
                    String redisKey=String.format("c#device#%s#%s",appId,deviceId);
                    dataMap.put(redisKey,json.toJSONString());
                    if(dataMap.size()==5000){
                        redisCompressionUtil.setBatchWithZstd(dataMap);
                        log.info("device write ok ,size  "+ dataMap.size());
                        dataMap.clear();
                    }
                });
                redisCompressionUtil.setBatchWithZstd(dataMap);
                log.info("device write ok ,size  "+ dataMap.size());
                dataMap.clear();
                break;
            }
            case "user_dict_meta":{
                starRocksJdbcUtil.streamQuery(tableinfo, size, rs -> {
                    JSONObject json=new JSONObject();
                    String userId = rs.getString("user_id");
                    json.put("has_init",rs.getBoolean("has_init"));
                    json.put("first_event",JSONObject.parseObject(rs.getString("first_event")));
                    json.put("first_field",JSONObject.parseObject(rs.getString("first_field")));
                    json.put("event_append",JSONObject.parseObject(rs.getString("event_append")));
                    String redisKey=String.format("c#user#%s#%s",appId,userId);
                    dataMap.put(redisKey,json.toJSONString());
                    if(dataMap.size()==5000){
                        redisCompressionUtil.setBatchWithZstd(dataMap);
                        log.info("user write ok ,size  "+ dataMap.size());
                        dataMap.clear();
                    }
                });
                redisCompressionUtil.setBatchWithZstd(dataMap);
                log.info("user last batch ok ,size  "+ dataMap.size());
                dataMap.clear();
                break;
            }
//            case "role_dict_meta":{
//                starRocksJdbcUtil.streamQuery("SELECT * FROM "+db+"."+table+" where app_id = "+appId, size, rs -> {
//                    JSONObject json=new JSONObject();
//                    String[] uniqueKey = rs.getString("role_id").split("#");
//                    json.put("has_init",rs.getBoolean("has_init"));
//                    json.put("first_event",JSONObject.parseObject(rs.getString("first_event")));
//                    json.put("first_field",JSONObject.parseObject(rs.getString("first_field")));
//                    json.put("event_append",JSONObject.parseObject(rs.getString("event_append")));
//                    String redisKey=String.format("c#role#%s#%s#%s",appId,uniqueKey[0],uniqueKey[1]);
//                    dataMap.put(redisKey, json.toString());
//                });
//                break;
//            }
            default:
                break;
        }

//
//        dataMap.forEach( (k,v) -> {
//            String s = redisCompressionUtil.get(k);
//            RedisCacheObj redisCacheObj = JSONObject.parseObject(s, RedisCacheObj.class);
//            System.out.println("parse data is -> " + redisCacheObj);
//        } );


        System.exit(0);
    }
}
