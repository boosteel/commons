package com.boosteel.util.support;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*

유니코드를 한글로 변환
new String (uni.getBytes("8859_1"),"KSC5601");

한글을 유니코드로 변환
new String (uni.getBytes("KSC5601"),"8859_1");

 */
public class Strings {

    public interface REPLACE_HANDLER {
        String replace(String[] groups, int index) throws Exception;
    }

    public static final String replace(String target, String pattern, REPLACE_HANDLER handler) {
        return replace(target, Pattern.compile(pattern), handler);
    }
    public static final String replace(String target, Pattern pattern, REPLACE_HANDLER handler) {
        Matcher m = pattern.matcher(target);
        StringBuffer buf = new StringBuffer();
        int count = 0, len;
        String[] values;

        try {
            while (m.find()) {
                len = m.groupCount() + 1;
                values = new String[len];
                for (int i = 0; i < len; i++) {
                    values[i] = m.group(i);
                }
                m.appendReplacement(buf, handler.replace(values, count++));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        m.appendTail(buf);
        return buf.toString();
    }

    private static final Pattern r_unicode = Pattern.compile("\\\\u(\\w{4})");

    public static String unicodeDecoder(String target) {
        Matcher m = r_unicode.matcher(target);
        StringBuffer buf = new StringBuffer();

        while (m.find()) {
            m.appendReplacement(buf, String.valueOf((char) Integer.parseInt(m.group(1), 16)));
        }

        m.appendTail(buf);
        return buf.toString();
    }

    private static Map<String, String> HTML_ESCAPE = new HashMap<>();
    private static Pattern R_HTML_ESCAPE = Pattern.compile("&\\w+;");

    static {
        HTML_ESCAPE.put("&quot;", "\"");
        HTML_ESCAPE.put("&amp;", "&");
        HTML_ESCAPE.put("&lt;", "<");
        HTML_ESCAPE.put("&gt;", ">");
        HTML_ESCAPE.put("&nbsp;", " ");
        HTML_ESCAPE.put("&#034;", "\"");
        HTML_ESCAPE.put("&rsquo;", "'");

    }

    public static final String unEscapeHTML(String target) {
        Matcher m = R_HTML_ESCAPE.matcher(target);
        StringBuffer buf = new StringBuffer();
        String r;
        while (m.find()) {
            r = HTML_ESCAPE.get(m.group());
            if (r != null)
                m.appendReplacement(buf, r);
        }

        m.appendTail(buf);
        return buf.toString();
    }

    public static final String eraseWindowStr(String target) {
        return target.replaceAll("\\\\|/|:|<|>|\\?|\\*|\"|\\|", "");
    }
}
