package com.flipkart.yaktest.failtest.utils;

import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.interruption.commons.RemoteService;
import com.flipkart.yaktest.interruption.models.YakComponent;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.util.List;

public class NameNodeUtil {

    private NameNodeUtil() {
    }

    public static String getActiveNamenode() {
        List<String> namenodes = Config.getInstance().getYakComponentConfig().get(YakComponent.NAME_NODE);

        return namenodes.stream().filter(namenode -> {
            String url = String.format("http://%s:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus", namenode);

            ResponseBody responseBody = null;
            try {
                responseBody = RemoteService.get(url);
                if (responseBody.string().contains("active")) {
                    return true;
                }
            } catch (IOException e) {
                return false;
            }
            return false;
        }).findFirst().orElse("");

    }
}
