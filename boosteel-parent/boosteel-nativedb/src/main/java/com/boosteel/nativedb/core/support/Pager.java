package com.boosteel.nativedb.core.support;

import java.util.List;
import java.util.Map;

public class Pager {

    protected int page = 1;
    protected int size = 10;
    protected String[] orders;
    protected int totalPages;
    protected long totalElements;
    protected List<Map<String, Object>> contents;

    private String ambiguous = "this";   // ORDER BY의 멤버에 붙일 key

    public Pager(Map<String, String> map) {
        String v = map.get("page");
        if (v != null && !v.isEmpty())
            page = Integer.parseInt(v);

        if ((v = map.get("size")) != null && !v.isEmpty()) {
            size = Integer.parseInt(v);
        }
        if ((v = map.get("order")) != null && !v.isEmpty()) {
            orders = v.split(",");
        }
    }

    public Pager(int page, int size) {
        this(page, size, null);
    }

    public Pager(int page, int size, String[] orders) {
        this.page = page;
        this.size = size;
        this.orders = orders;
    }

    public void setAmbiguous(String ambiguous) {

    }

    public String orderBy() {
        int len = orders == null ? 0 : orders.length;
        if (len > 0) {
            String[] values = new String[len];
            String val;
            for (int i = 0; i < len; i++) {
                val = orders[i].replaceAll("^<|>", "");
                val = val.indexOf(".") == -1 ? ambiguous + "." + val : val;
                values[i] = val + (orders[i].charAt(0) == '<' ? " DESC" : " ASC");
            }
            return " ORDER BY " + String.join(", ", values);
        } else {
            return "";
        }
    }

    public String limit() {
        return " LIMIT " + ((page - 1) * size) + ", " + size;
    }

    public Pager setTotalElements(long totalElements) {
        totalPages = (int) totalElements / size;
        if (totalElements % size != 0) totalPages++;

        return this;
    }

    public Pager setContents(List contents) {
        this.contents = contents;
        return this;
    }

    public int getPage() {
        return page;
    }

    public int getSize() {
        return size;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public List getContents() {
        return contents;
    }
}
