package com.linkflow.api;

import cn.hutool.core.date.DateUtil;
import cn.hutool.http.HttpException;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/3/10 1:32 下午
 */
public class LinkflowManager {
    private static Logger logger = Logger.getLogger(LinkflowManager.class);

    public static String AUTH_URL = "token";
    public static String IDENTIFIERS_URL = "identifiers";
    public static String UDE_URL = "ude";
    public static String BATH_URL = "https://open.linkflowtech.com/";
    public volatile String token;

    private String appKey;
    private String appSecret;
    private int timeoutMs = 2000;
    private long expiration = 0L;
    private long successCount = 0L;

    public LinkflowManager(String appKey, String appSecret) {
        this(appKey, appSecret, 2000);
    }

    public LinkflowManager(String appKey, String appSecret, int timeoutMs) {
        this.appKey = appKey;
        this.appSecret = appSecret;
        this.timeoutMs = timeoutMs;
        logger.info("create LinkflowManager success");
    }


    synchronized public void getToken() {
        long currentTime = new Date().getTime();
        if (this.expiration > currentTime && this.expiration != 0) {
            return;
        }
        Map<String, String> headers = new HashMap<>();
        headers.put("X-OpenApi-Key", appKey);
        headers.put("X-OpenApi-Secret", appSecret);
        HttpResponse response = HttpRequest.get(BATH_URL + AUTH_URL)
                .headerMap(headers, true)
                .timeout(this.timeoutMs)
                .execute();
        if (response.isOk()) {
            JSONObject tokenObj = JSON.parseObject(response.body());
            this.token = tokenObj.getString("token");
            this.expiration = tokenObj.getLong("expiration");
        } else {
            throw new RuntimeException("linkflow login failed");
        }
    }

    private Map<String, String> getAuthHeader() {
        Map<String, String> headers = new HashMap<>();
        if (StringUtils.isBlank(this.token)) {
            getToken();
        }
        long currentTime = new Date().getTime();
        if (this.expiration < currentTime) {
            getToken();
        }
        headers.put("X-OpenApi-Token", this.token);
        return headers;
    }

    public boolean insertOrUpdateIdentifiers(String jsonData) {
        Map<String, String> headers = getAuthHeader();
        try {
            HttpResponse response = HttpRequest.post(BATH_URL + IDENTIFIERS_URL)
                    .headerMap(headers, true)
                    .body(jsonData)
                    .timeout(timeoutMs)
                    .execute();
            if (response.isOk()) {
//                logger.info("update identifier success, return data: " + response.body());
                return true;
            } else {
                JSONObject result = JSON.parseObject(response.body());
                logger.error("update identifier error, source msg: " + jsonData + ", error msg: " + response.body());
                return false;
            }
        } catch (HttpException e) {
            logger.error(ExceptionUtils.getStackTrace(e));
        }
        return false;
    }

    public boolean insertOrUpdateUde(String jsonData) {
        Map<String, String> headers = getAuthHeader();
        try {
            HttpResponse response = HttpRequest.post(BATH_URL + UDE_URL)
                    .headerMap(headers, true)
                    .body(jsonData)
                    .timeout(timeoutMs)
                    .execute();
            if (response.isOk()) {
//                logger.info("update ude success, return data: " + response.body());
                return true;
            } else {
                JSONObject result = JSON.parseObject(response.body());
                logger.error("update ude error, source msg: " + jsonData + ",error msg: " + response.body());
                return false;
            }
        } catch (HttpException e) {
            logger.error(ExceptionUtils.getStackTrace(e));
        }
        return false;
    }

    public static void main(String[] args) {
        LinkflowManager linkflowManager = new LinkflowManager("WO8DY6lIuQ0tFtUZ", "g6NpkVDBvXr12LNEO8osGPWzWqnJT3dT");
        linkflowManager.getToken();
        linkflowManager.getAuthHeader();
    }
}
