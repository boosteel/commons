package com.boosteel.util.support;

import com.boosteel.util.IAccess;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MapAccess implements IAccess<Map<String, Object>> {


    public static final Map<String, String> createStringMap(String...args) {
        Map<String, String> map = new HashMap<>();
        int i = 0, len = args.length;
        while(i < len) {
            map.put(args[i++], args[i++]);
        }
        return map;
    }
    public static final Map<String, Object> createDataMap(Object...args) {
        Map<String, Object> map = new HashMap<>();
        int i = 0, len = args.length;
        while(i < len) {
            map.put(args[i++].toString(), args[i++]);
        }
        return map;
    }

    public static final <T> T get(Map<String, Object> map, String key) {

        String[] keys = key.split("\\.");
        int len = keys.length, i = 0;

        Object val = map;

        for (; i < len; i++) {
            if (val != null && Map.class.isAssignableFrom(val.getClass())) {
                val = ((Map) val).get(keys[i]);
            } else return null;
        }
        return (T) val;
    }

    public static final Map<String, Object> put(Map<String, Object> map, String key, Object value) {
        Map<String, Object> target = map;
        String[] keys = key.split("\\.");

        int i = 0, l = keys.length - 1;
        for (; i < l; i++) {
            if (target.get(key = keys[i]) == null)
                target.put(key, new HashMap<>());
            target = (Map<String, Object>) target.get(key);
        }
        target.put(keys[i], value);
        return map;
    }

    private Map<String, Object> map;

    public MapAccess() {
        this(new HashMap<>());
    }

    public MapAccess(Map<String, Object> map) {
        this.map = map;
    }

    @Override
    public Map<String, Object> target() {
        return map;
    }

    @Override
    public Object get(String key) {
        return map.get(key);
    }

    @Override
    public IAccess<Map<String, Object>> set(String key, Object value) {
        put(map, key, value);
        return this;
    }

    @Override
    public Set<String> keySet() {
        return map.keySet();
    }
}
