package com.flipkart.yaktest.interruption.commons;

import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@SuppressWarnings("WeakerAccess")
public class RemoteService {

    private static Logger logger = LoggerFactory.getLogger(RemoteService.class);
    private static OkHttpClient client = new OkHttpClient.Builder().build();

    public static ResponseBody get(String url) throws IOException {

        Request.Builder requestBuilder = new Request.Builder().url(url).addHeader("Content-Type", "application/json");
        Request request = requestBuilder.build();

        Response response = client.newCall(request).execute();
        return getResponseBody(response, url);
    }

    public static ResponseBody get(String url, Headers headers) throws IOException {

        Request.Builder requestBuilder = new Request.Builder().url(url).addHeader("Content-Type", "application/json").headers(headers);
        Request request = requestBuilder.build();

        Response response = client.newCall(request).execute();
        return getResponseBody(response, url);
    }

    public static ResponseBody post(String url, RequestBody requestBody, Headers headers) throws IOException {

        Request.Builder requestBuilder = new Request.Builder().url(url).headers(headers).post(requestBody);
        Request request = requestBuilder.build();

        Response response = client.newCall(request).execute();
        return getResponseBody(response, url);
    }

    public static ResponseBody getResponseBody(Response response, String url) {
        ResponseBody responseBody = response.body();
        if (responseBody == null) {
            throw new RuntimeException("Unable to fetch response for service call " + url);
        }
        return responseBody;
    }
}
