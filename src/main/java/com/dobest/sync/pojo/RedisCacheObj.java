package com.dobest.sync.pojo;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collections;
import java.util.Map;

@Data
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class RedisCacheObj {

    private long iid;
    private boolean hasInit;
    private Map<String, String> firstEvent = Collections.emptyMap();
    private Map<String, String> firstField = Collections.emptyMap();
    private JSONObject eventAppend = new JSONObject();


    public String toJsonString() {
        return JSON.toJSONString(this);
    }
}
