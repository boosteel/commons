package com.boosteel.nativedb.core;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataConverter {

    /*
     *  자바 타입에 따라 SQL 문자열 값으로 변환해준다.
     *  Date ==>  'yyyy-MM-dd HH:mm:ss'
     *  String ==> 'value'
     *  int, long... ==>  value
     */
    public static String sql_by_jType(Object val) {
        if (val == null) return "null";

        Class<?> clazz = val.getClass();

        if (CharSequence.class.isAssignableFrom(clazz))
            return "'" + val.toString().replaceAll("('|\\\\)", "\\\\$1") + "'";

        if (Date.class.isAssignableFrom(clazz))
            return "'" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date) val) + "'";

        return val.toString();
    }


    // sql type ==> java type
    public static Object convertJSON(ResultSet rs, String dataType, int index) throws Exception {
        switch (dataType) {
            case "TIMESTAMP":
            case "DATETIME":
            case "DATE":
            case "TIME":
            case "YEAR":
                Timestamp stamp = rs.getTimestamp(index);
                return stamp == null ? null : stamp.getTime();
            default:
                return data_by_dType(rs, dataType, index);
        }
    }

    // 요구하는 자바 타입에 맞춰서 데이터 가지고 오기
    public static Object data_by_jType(ResultSet rs, ResultSetMetaData meta, String type, int index) throws Exception {
        switch (type) {
            case "java.lang.Integer":
            case "int":
                return rs.getInt(index);
            case "java.lang.Long":

                /*
                 *   날짜 타입인데 long으로 잡혀있으면 getTime()으로 변환한다.
                 */
            case "long":
                switch (meta.getColumnTypeName(index)) {
                    case "TIMESTAMP":
                    case "DATETIME":
                    case "DATE":
                    case "TIME":
                    case "YEAR":
                        Timestamp t = rs.getTimestamp(index);
                        return t == null ? 0l : t.getTime();
                    default:
                        return rs.getLong(index);
                }

            case "java.util.Date":
                Timestamp t = rs.getTimestamp(index);
                return t == null ? null : new Date(t.getTime());
            case "java.lang.Boolean":
            case "boolean":
                return rs.getBoolean(index);
            case "java.lang.String":
                return rs.getString(index);
            default:
                return rs.getObject(index);
        }
    }

    /*
     *  @INSERT 혹은 @UPDATE 용
     */
    public static String dType_to_string(String type, Object value) {
        if (value == null) return null;

        if (value instanceof String) return _dType_to_string(type, value.toString());
        return sql_by_jType(value);

    }

    public static String _dType_to_string(String type, String value) {
        switch (type) {
            case "TIMESTAMP":
            case "DATETIME":
            case "DATE":
            case "TIME":
            case "YEAR":
            case "CHAR":
            case "VARCHAR":
            case "TEXT":
                return "'" + value + "'";
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
            case "BIGINT":
            case "BIT":
            case "BOOL":
            case "BOOLEAN":
            case "DECIMAL":
            case "DEC":
            case "NUMERIC":
            case "FIXED":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "FLOAT":
                if(value.isEmpty()) return "0";
            default:
                return value;
        }
    }


    // core 데이터타입에 맞춰 데이터 변환하기
    public static Object data_by_dType(ResultSet rs, String dataType, int index) throws Exception {
        switch (dataType) {
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
                return rs.getInt(index);
            case "BIGINT":
                return rs.getLong(index);
            case "BIT":
            case "BOOL":
            case "BOOLEAN":
                return rs.getBoolean(index);
            case "DECIMAL":
            case "DEC":
            case "NUMERIC":
            case "FIXED":
                return rs.getBigDecimal(index);
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return rs.getDouble(index);
            case "FLOAT":
                return rs.getFloat(index);

            /*
             *  JSON 변환을 위해 날짜타입은 숫자형으로 내보낸다.
             */
            case "TIMESTAMP":
            case "DATETIME":
            case "DATE":
            case "TIME":
            case "YEAR":
                Timestamp stamp = rs.getTimestamp(index);
                return stamp == null ? null : stamp.getTime();
            default:
                return rs.getString(index);
        }
    }

    private static String quite(String s) {
        return "'" + s + "'";
    }

    // PreparedStatement용
    public static PreparedStatement setPstmt(PreparedStatement pstmt, int index, String type, Object value) throws SQLException {
        switch (type) {
            case "d":
            case "date":
                if (value == null) pstmt.setNull(index, Types.DATE);

                // long타입일 경우 Date객체로 만든다.
                String className = value.getClass().getName();
                if (className.equals("long") || className.equals("java.lang.Long"))
                    value = new Date((long) value);

                pstmt.setString(index, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date) value));
                break;
            case "int":
            case "i":
                if (value == null) pstmt.setNull(index, Types.INTEGER);
                else pstmt.setInt(index, (int) value);
                break;
            case "long":
            case "l":
                if (value == null) pstmt.setNull(index, Types.BIGINT);
                else pstmt.setLong(index, (long) value);
                break;
            case "%like%":
                pstmt.setString(index, "%" + value.toString().replaceAll("%", "\\\\%") + "%");
                break;
            case "%like":
                pstmt.setString(index, "%" + value.toString().replaceAll("%", "\\\\%"));
                break;
            case "like%":
                pstmt.setString(index, value.toString().replaceAll("%", "\\\\%") + "%");
                break;
            default:
                if (value == null) pstmt.setNull(index, Types.VARCHAR);
                pstmt.setString(index, value.toString());
        }
        return pstmt;
    }

    /*
     *   Statement용 변환기
     *   value가 String이어도 문제가 없다.
     *
     */
    public static String sql_by_exp(String type, Object value) {
        if (value == null) return null;

        String v;

        switch (type) {
            case "d":
            case "date":
                switch (value.getClass().getName()) {
                    case "int":
                    case "long":
                    case "java.lang.Integer":
                    case "java.lang.Long":
                        return quite(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date((int) value)));
                    case "java.util.Date":
                        return quite(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date) value));
                    default:
                        return value.toString();
                }
            case "%like%":
                return quite("%" + value.toString().replaceAll("(%|'|\\\\)", "\\\\$1") + "%");
            case "%like":
                return quite("%" + value.toString().replaceAll("(%|'|\\\\)", "\\\\$1"));
            case "like%":
                return quite(value.toString().replaceAll("(%|'|\\\\)", "\\\\$1") + "%");
            case "int":
            case "i":
            case "long":
            case "l":
                return value.toString();
            default:
                return quite(value.toString().replaceAll("('|\\\\)", "\\\\$1"));
        }
    }

}
