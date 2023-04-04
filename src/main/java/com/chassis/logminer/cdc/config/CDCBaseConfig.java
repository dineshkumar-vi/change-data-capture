package com.chassis.logminer.cdc.config;

import lombok.Data;

import java.util.List;

@Data
public class CDCBaseConfig {

    private String connectionUrl;
    private String userName;
    private String password;
    private String schema;
    private List<String> tableIncludeList;

}
