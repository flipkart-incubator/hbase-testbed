package com.flipkart.yaktest.failtest.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.interruption.commons.RemoteService;
import com.flipkart.yaktest.interruption.models.YakComponent;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HMasterUtil {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static String getActiveMaster() throws IOException {
        List<String> masterNodes = Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER);

        return masterNodes.stream().filter(master -> {
            String url = String.format("http://%s:16010/jmx?qry=Hadoop:service=HBase,name=Master,sub=Server", master);

            ResponseBody responseBody = null;
            try {
                responseBody = RemoteService.get(url);

                String responseString = responseBody.string();
                Map<String, List<Map<String, Object>>> data =
                        objectMapper.readValue(responseString, new TypeReference<Map<String, List<Map<String, Object>>>>() {
                        });
                String isActive = (String) data.get("beans").get(0).get("tag.isActiveMaster");
                if ("true".equals(isActive)) {
                    return true;
                }

            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            return false;
        }).findFirst().orElseGet(null);

    }

    public static void main(String args[]) throws IOException {
        System.out.println(getActiveMaster());
    }
}
