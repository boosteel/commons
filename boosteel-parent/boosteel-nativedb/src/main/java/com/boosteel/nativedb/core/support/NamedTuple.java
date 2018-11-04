package com.boosteel.nativedb.core.support;

public class NamedTuple {
    int index;
    String type;
    String name;

    public NamedTuple(String n, int i,  String t) {
        index = i;
        type = t;
        name = n;
    }

    @Override
    public String toString() {
        return "[" + index + "] " + type;
    }
}
