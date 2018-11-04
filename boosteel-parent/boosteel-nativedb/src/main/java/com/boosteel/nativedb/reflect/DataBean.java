package com.boosteel.nativedb.reflect;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.boosteel.nativedb.core.DataConverter.data_by_jType;
import static com.boosteel.nativedb.core.DataConverter.sql_by_jType;


/*
 *   Hibernate의 Domain객체를 떠올리면 된다.
 *   리플렉션을 이용해 객체를 Map처럼 이용한다.
 *
 *
 */
public class DataBean extends ReflectObject {

    /*
     *  모든 BeanData는 캐시에 저장된다.
     *  Class.getName()은 유일한 값이므로 getName()으로 캐시에 저장한다.
     */
    private static final Map<String, DataBean> $cache = new HashMap<>();

    public static final DataBean getBeanData(Class<?> clazz) {
        return getBeanData(clazz.getName());
    }

    public static final DataBean getBeanData(String className) {
        DataBean db = $cache.get(className);
        if (db == null) {
            try {
                db = new DataBean(Class.forName(className));
                for(Class<?> type : db.setterTypes.values())
                    $assert(type.getName());
                $cache.put(className, db);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return db;
    }


    private static String $assert(String type) {
        switch(type) {
            case "long" :
            case "int" :
            case "char" :
                throw new RuntimeException("int, long 등 primitive type은 DataBean의 멤버로 쓸 수 없습니다.\n" +
                        "primitive는 기본값이 있으므로 null값 체크가 불가능하기 때문");
        }
        return type;
    }

    private DataBean(Class<?> clazz) {
        super(clazz);
    }

    public String dynamicInsert(String tableName, Object target) throws Exception {

        StringBuilder builder = new StringBuilder("INSERT INTO ")
                .append(tableName)
                .append(" ");
        Object value;
        List<String> props = new ArrayList<>(),
                values = new ArrayList<>();

        for(String key : getterMap.keySet()) {
            if((value = getterMap.get(key).get(target)) != null) {
                props.add(key);
                values.add(sql_by_jType(value));
            }
        }

        return builder.append("(").append(String.join(", ", props)).append(") VALUES ")
                .append("(").append(String.join(", ", values)).append(")")
                .toString();
    }

    public String dynamicUpdate(String tableName, Object target) throws Exception {

        StringBuilder builder = new StringBuilder("UPDATE ")
                .append(tableName)
                .append(" SET ");
        Object value;
        List<String> values = new ArrayList<>();
        String where = " WHERE id = ";

        for(String key : getterMap.keySet()) {
            if(key.equals("id")) {
                Getter getter = getterMap.get("id");
                if(getter == null)
                    throw new RuntimeException("UPDATE 문을 사용하기 위해서는 DataBean에 id 멤버가 반드시 존재해야 합니다.");
                value = getter.get(target);
                if(value == null)
                    throw new RuntimeException("UPDATE 문에서 id는 절대 null이면 안됩니다.");
                where += sql_by_jType(value);

            }
            else if((value = getterMap.get(key).get(target)) != null) {
                values.add(key + " = " + sql_by_jType(value));
            }
        }

        return builder.append(String.join(", ", values))
                .append(where).toString();
    }

    // ResultSet을 받아 Object를 채운다.
    public <T> T newInstance(ResultSet rs) {
        try {
            T $self = (T) clazz.newInstance();

            Map<String, Capsule> dataBeanMap = new HashMap<>();

            ResultSetMetaData meta = rs.getMetaData();
            int len = meta.getColumnCount();
            String label, tableName;
            Class<?> setterType;

            while (len > 0) {
                label = meta.getColumnLabel(len);
                tableName = meta.getTableName(len);
                setterType = setterTypes.get(label);

                /*
                 *  tableName을 통해 객체를 선별한다.
                 *  이때 해당 타입멤버가 존재하는지 확인한다. :: setterType
                 */
                if (setterType != null) {
                    if (tableName.equals("$")) {
                        set($self, label, data_by_jType(rs, meta, setterType.getName(), len));
                    }
                    // 하위 멤버 빈일 경우
                    else {
                        Capsule capsule = dataBeanMap.get(tableName);
                        if (capsule == null)
                            dataBeanMap.put(tableName, capsule = new Capsule(setterType.getName()));
                        capsule.set(label, rs, meta, len);
                    }
                }

                len--;
            }

            /*
             *  모든 값이 셋팅된 객체를 넣어준다.
             */
            for (Map.Entry<String, Capsule> capsuleEntry : dataBeanMap.entrySet()) {
                setterMap
                        .get(capsuleEntry.getKey())
                        .set($self, capsuleEntry.getValue().target);
            }

            return $self;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
     *  ResultSet 셋팅 중, 멤버 객체의 값 매핑을 위해 사용한다.
     */
    class Capsule {
        String className;
        DataBean dataBean;
        Object target;

        Capsule(String className) throws Exception {
            this.className = className;
            dataBean = DataBean.getBeanData(className);
            target = dataBean.clazz.newInstance();
        }

        void set(String name, ResultSet rs, ResultSetMetaData meta, int index) throws Exception {
            Setter setter = dataBean.setterMap.get(name);
            if (setter != null) setter.set(target, data_by_jType(rs, meta, className, index));
        }
    }
}
