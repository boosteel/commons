package com.boosteel.nativedb.core;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

public class ResultSetAccess {

    public static final <T> T reduce(ResultSet rs, T d, SQL.ReduceHandlerR<T> handler) {
        int index = 0;
        try {
            while (rs.next())
                d = handler.accept(d, rs, index++);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return d;
    }

    public static final <T> T reduce(ResultSet rs, T d, SQL.ReduceHandler<T> handler) {
        int index = 0;
        try {
            while (rs.next())
                handler.accept(d, rs, index++);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return d;
    }

    // 먼저 rs.next() 이후에 넣어줘야 한다.
    public static final List<Map<String, Object>> readAll(ResultSet rs) throws Exception {
        return readAll(rs, DataConverter::data_by_dType);
    }

    public static final List<Map<String, Object>> readAllJSON(ResultSet rs) throws Exception {
        return readAll(rs, DataConverter::convertJSON);
    }

    public static final List<Map<String, Object>> readAll(ResultSet rs, SQL.CONVERT_HANDLER handler) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();

        if (rs.next()) {
            ResultSetMetaData meta = rs.getMetaData();
            do {
                result.add($read(rs, meta, handler));
            } while (rs.next());
        }
        return result;
    }

    public static final Map<String, Object> readJSON(ResultSet rs) throws Exception {
        return read(rs, DataConverter::convertJSON);
    }

    public static final Map<String, Object> read(ResultSet rs) throws Exception {
        return read(rs, DataConverter::data_by_dType);
    }

    public static final Map<String, Object> read(ResultSet rs, SQL.CONVERT_HANDLER handler) throws Exception {
        return rs.next() ? $read(rs, rs.getMetaData(), handler) : Collections.EMPTY_MAP;
    }

    private static final Map<String, Object> $read(ResultSet rs, ResultSetMetaData meta, SQL.CONVERT_HANDLER handler) throws Exception {
        Map<String, Object> map = new HashMap<>(), target;
        int len = meta.getColumnCount() + 1;
        String label = null;
        String[] labels;

        while (len-- > 1) {

            // .으로 이루어진 건 하위맵으로
            target = map;
            label = meta.getColumnLabel(len);
            labels = label.split("\\.");

            int i = 0, l = labels.length - 1;
            for (; i < l; i++) {
                if (target.get(labels[i]) == null)
                    target.put(labels[i], new HashMap<>());
                target = (Map<String, Object>) target.get(label = labels[i]);
            }

            label = labels[i];

            target.put(
                    label,
                    handler.convert(rs, meta.getColumnTypeName(len), len)
            );
        }
        return map;
    }
}
