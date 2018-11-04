package com.boosteel.nativedb.core;


import com.boosteel.nativedb.core.support.NamedPreparedStatementFactory;
import com.boosteel.nativedb.core.support.NamedTuple;
import com.boosteel.util.IAccess;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.boosteel.nativedb.core.DataConverter.dType_to_string;
import static com.boosteel.nativedb.core.DataConverter.sql_by_exp;
import static com.boosteel.nativedb.core.DataConverter.sql_by_jType;

public class SQL {


    /*
     *  Map이 가지고 있는 properties를 이용해 SQL을 작성한다.
     *  Map의 value값이 가진 Java타입에 따라 SQL 데이터타입이 결정된다.
     */
    public static final String insert(IAccess iAccess, TableInfo info) {

        Set<String> keySet = iAccess.keySet();

        List<String>
                columns = new ArrayList<>(),
                values = new ArrayList<>();

        Set<String> props = info.props();


        for (String key : keySet) {
            if (props.contains(key)) {
                columns.add(key);
                values.add(dType_to_string(info.getType(key), iAccess.get(key)));
            }

        }
        return new StringBuffer("(").append(String.join(", ", columns)).append(") VALUES (")
                .append(String.join(", ", values)).append(")")

                .toString();
    }

    public static final String insert(String tableName, Map<String, Object> value) {
        List<Map<String, Object>> values = new ArrayList<>();
        values.add(value);
        return insert(tableName, values);
    }

    public static final String insert(String tableName, List<Map<String, Object>> values) {
        Set<String> keys = values.get(0).keySet();
        StringBuffer buf = new StringBuffer("INSERT INTO ")
                .append(tableName)
                .append(" (")
                .append(columns(keys))
                .append(") values ");
        return values(keys, values, buf).toString();
    }


    /*
     *   WHERE id = {val} 로만 업데이트 할때 쓴다.
     *   value는 반드시 id를 가지고 있어야 한다.
     */
    public static final String update(IAccess<?> value, TableInfo info) {

        StringBuffer buf = new StringBuffer();
        String id = info.primaryKey;
        Set<String> props = info.props();

        for (String key : value.keySet()) {
            if (!key.equals(id) && props.contains(key))
                buf.append(key).append(" = ").append(dType_to_string(info.getType(key), value.get(key))).append(", ");
        }

        return buf.delete(buf.length() - 2, buf.length()).toString();
    }


    // ****************

    public static final String selectPrefix(String str, String prefix) {
        prefix = prefix + ".";
        return prefix + str.replaceAll("\\s+", ", " + prefix);
    }


    // 컬럼명 기재
    public static StringBuffer columns(Iterable<String> keys) {
        return columns(keys, new StringBuffer());
    }

    public static StringBuffer columns(Iterable<String> keys, StringBuffer buffer) {
        return buffer.append(String.join(", ", keys));
    }


    // values 이하 작성
    public static StringBuffer values(Map<String, Object> values) {
        return values(values.keySet(), values, new StringBuffer());
    }

    public static StringBuffer values(Set<String> keys, Map<String, Object> values) {
        return values(keys, values, new StringBuffer());
    }

    public static StringBuffer values(Map<String, Object> values, StringBuffer buffer) {
        return values(values.keySet(), values, buffer);
    }

    public static StringBuffer values(Set<String> keys, Map<String, Object> values, StringBuffer buffer) {
        buffer.append("(");
        for (String key : keys) {
            buffer.append(sql_by_jType(values.get(key))).append(",");
        }
        buffer.delete(buffer.length() - 1, buffer.length()).append(")");
        return buffer;
    }

    // 여러개
    public static StringBuffer values(Set<String> keys, List<Map<String, Object>> values, StringBuffer buffer) {
        for (Map<String, Object> v : values)
            values(keys, v, buffer).append(", ");
        return buffer.delete(buffer.length() - 2, buffer.length());
    }


    // ************************************** SQL 작성용  ************************************** //

    private static final Pattern
            p = Pattern.compile(":([^:\\s,()]+)"),
            point = Pattern.compile(" \\[([^\\]]+)\\]|:([^:\\s,()]+)");  // [prop = :value] 또는 :value


    /*
     *  preparedStatement의 ?를 :name으로 사용하기 위한 parsing 로직
     *
     */
    public static NamedPreparedStatementFactory newPrepared(String sql) {

        StringBuffer buf = new StringBuffer();
        List<NamedTuple> order = new ArrayList<>();

        Matcher m = p.matcher(sql);
        int index = 1, t1;

        while (m.find()) {

            // :name ==> ?
            m.appendReplacement(buf, "?");

            String val = m.group(1), type = "";

            // 타입이 있을 경우
            if ((t1 = val.indexOf("[")) != -1) {
                type = val.substring(t1 + 1, val.indexOf("]"));
                val = val.substring(0, t1);
            }
            // like
            else if (val.contains("%")) {
                if (val.startsWith("%")) {
                    if (val.endsWith("%")) {
                        type = "%like%";
                    } else type = "%like";
                } else {
                    type = "like%";
                }

                val = val.replaceAll("%", "");

            }

            order.add(new NamedTuple(val, index++, type));
        }

        m.appendTail(buf);
        sql = buf.toString();

        return new NamedPreparedStatementFactory(sql, order);
    }


    /*
     *    SELECT * FROM
     */
    public static final DQuery dynamicSQL(String sql) {
        List<Function<IAccess<?>, String>> list = new ArrayList<>();
        int pos = 0, i;
        Matcher m = point.matcher(sql);

        while (m.find()) {

            String key = m.group(2),
                    text = sql.substring(pos, pos = m.start());

            list.add(obj -> text);      // *before text

            // ① [AND prop = :value{d}]   or [AND prop BETWEEN :st AND :et]
            if (key == null) {
                list.add(checking(m.group(1)));
            } else {
                list.add(keyApply(key));
            }

            pos = m.end();
        }

        if (pos < sql.length()) {
            String text = sql.substring(pos, sql.length());
            list.add((a) -> text);
        }


        return (map) -> {
            StringBuilder builder = new StringBuilder();
            for (Function<IAccess<?>, String> g : list)
                builder.append(g.apply(map));
            return builder.toString();
        };
    }


    private static final Function<IAccess<?>, String> checking(String str) {

        int pos = 0;
        List<Function<IAccess<?>, String>> handlers = new ArrayList<>();

        Matcher m = p.matcher(str);

        while (m.find()) {
            String text = str.substring(pos, m.start());
            handlers.add(values -> text);

            handlers.add(SQL.keyApply(m.group(1)));

            pos = m.end();
        }

        if (pos < str.length()) {
            String text = str.substring(pos, str.length());
            handlers.add((a) -> text);
        }

        return map -> {
            StringBuffer buf = new StringBuffer(" ");
            String v;
            for (Function<IAccess<?>, String> handler : handlers)
                if ((v = handler.apply(map)) == null) return "";
                else buf.append(v);

            return buf.toString();
        };
    }

    // name{?}
    public static final Function<IAccess<?>, String> keyApply(String exp) {
        int i = exp.indexOf("{");
        String key = exp, type = null;
        boolean isLike, isList;

        if (key.startsWith("...")) {
            exp = exp.substring(3);
            isList = true;
            i -= 3;
        } else isList = false;

        // 데이터 타입이 정의되어 있을때
        if (i != -1) {
            key = exp.substring(0, i);
            type = exp.substring(i + 1, exp.indexOf("}"));
            isLike = false;
        }
        // LIKE
        else if (isLike = key.contains("%")) {
            if (key.startsWith("%")) {
                type = key.endsWith("%") ? "%like%" : "%like";
            } else {
                type = "like%";
            }
            key = key.replaceAll("%", "");
        }

        String $key = key, $type = type;
        return (obj) -> {
            Object data = obj.get($key);
            if (data == null) return null;

            // :...prop  ==> List타입이 들어온다.  IN (1,2,3)  용
            if (isList) {
                List list = (List) obj.get($key);
                String[] values = new String[list.size()];
                int ii = 0;
                for (Object v : list) {
                    values[ii++] = $type == null ? sql_by_jType(v) : sql_by_exp($type, v);
                }
                return String.join(", ", values);
            }

            if (isLike) return data.toString().isEmpty() ? null : sql_by_exp($type, data);

            return $type == null ? sql_by_jType(obj.get($key)) : sql_by_exp($type, obj.get($key));
        };
    }

    public interface ReduceHandler<T> {
        void accept(T t, ResultSet rs, int index) throws Exception;
    }

    public interface ReduceHandlerR<T> {
        T accept(T t, ResultSet rs, int index) throws Exception;
    }

    public interface CONVERT_HANDLER {
        Object convert(ResultSet rs, String dataType, int index) throws Exception;
    }

    public interface DQuery {
        String apply(IAccess<?> map);
    }


}
