package com.boosteel.nativedb.core;

import com.boosteel.nativedb.NativeDB;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TableInfo {

    private String name;
    public String primaryKey;

    private Set<String> set;
    private Map<String, String> columns;

    private TableInfo(String name) {
        this.name = name;
    }

    public Set<String> props() {
        return set;
    }

    public String getType(String key) {
        return columns.get(key);
    }

    private static final Map<String, TableInfo> INFO_CACHE = new HashMap<>();
    public static final TableInfo create(String name, NativeDB db) {

        TableInfo v = INFO_CACHE.get(name);
        if(v == null)
            v = db.execute("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY " +
                        "FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='" + name + "' GROUP BY COLUMN_NAME",
                (rs) -> {
                    TableInfo info = new TableInfo(name);
                    Map<String, String> columns = info.columns = new HashMap<>();
                    String n;
                    while (rs.next()) {
                        columns.put(n = rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE").toUpperCase());
                        if(rs.getString("COLUMN_KEY").equals("PRI"))
                            info.primaryKey = n;
                    }
                    info.set = columns.keySet();

                    if(columns.isEmpty())
                        throw new RuntimeException(name + "은 존재하지 않는 테이블입니다.");

                    INFO_CACHE.put(name, info);
                    return info;
                });

        return v;
    }

}
