package com.boosteel.util;

import java.util.Set;

public interface IAccess<T> {
    T target();
    Object get(String key);
    IAccess<T> set(String key, Object value);
    Set<String> keySet();
}
