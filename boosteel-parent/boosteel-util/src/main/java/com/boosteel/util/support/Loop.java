package com.boosteel.util.support;

import java.util.Collection;

public class Loop {


    public static final <T, R> R reduce(Collection<T> t, R r, REDUCE_R<T, R> handler) {
        try {
            int index = 0;
            for(T v : t) {
                r = handler.accept(v, r, index++);
            }
            return r;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static final <T, R> R reduce(Collection<T> t, R r, REDUCE<T, R> handler) {
        try {
            int index = 0;
            for(T v : t)
                handler.accept(v, r, index++);
            return r;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public interface REDUCE_R<T, R> {
        R accept(T t, R r, int index) throws Exception;
    }
    public interface REDUCE<T, R> {
        void accept(T t, R r, int index) throws Exception;
    }
}
