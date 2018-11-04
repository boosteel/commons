package com.boosteel.nativedb.core;

import com.boosteel.nativedb.reflect.ReflectObject;
import com.boosteel.util.IAccess;
import com.boosteel.util.support.MapAccess;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.boosteel.nativedb.core.DataConverter.data_by_dType;

public class DataAccess {

    public static final List<Map<String, Object>> readAll(ResultSet rs) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            while (rs.next())
                result.add(read(rs, new MapAccess()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static final <T> List<T> readAll(ResultSet rs, Class<T> clazz) {
        List<T> result = new ArrayList<>();
        try {
            while (rs.next())
                result.add(read(rs, ReflectObject.createAccess(clazz.newInstance())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static final <T> T read(ResultSet rs, IAccess<T> access) {
        try {

            ResultSetMetaData meta = rs.getMetaData();
            int len = meta.getColumnCount();
            String label, tableName;

            while (len > 0) {
                label = meta.getColumnLabel(len);
                tableName = meta.getTableName(len);

                /*
                 *  tableName을 통해 객체를 선별한다.
                 *  _ 로 시작하는 table은 맵핑에서 제외한다.
                 */
                if (tableName.isEmpty())
                    access.set(label, data_by_dType(rs, meta.getColumnTypeName(len), len));
                else if (tableName.charAt(0) != '_') {
                    if (tableName.equals("this")) {
                        access.set(label, data_by_dType(rs, meta.getColumnTypeName(len), len));
                    }
                    // 하위 멤버 빈일 경우
                    else {
                        access.set(tableName + "." + label, data_by_dType(rs, meta.getColumnTypeName(len), len));
                    }
                }
                len--;
            }

            return access.target();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
