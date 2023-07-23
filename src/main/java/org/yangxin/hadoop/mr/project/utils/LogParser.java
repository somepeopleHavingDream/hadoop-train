package org.yangxin.hadoop.mr.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author yangxin
 * 2023/7/23 14:13
 */
public class LogParser {

    public Map<String, String> parse(String log) {
        if (StringUtils.isBlank(log)) {
            return Collections.emptyMap();
        }

        Map<String, String> info = new HashMap<>();
        IPParser ipParser = IPParser.getInstance();

        String[] split = log.split("\001");
        String ip = split[13];
        IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);

        String country = "-";
        String province = "-";
        String city = "-";
        if (Objects.nonNull(regionInfo)) {
            country = regionInfo.getCountry();
            province = regionInfo.getProvince();
            city = regionInfo.getCity();
        }

        info.put("ip", ip);
        info.put("country", country);
        info.put("province", province);
        info.put("city", city);

        return info;
    }
}
