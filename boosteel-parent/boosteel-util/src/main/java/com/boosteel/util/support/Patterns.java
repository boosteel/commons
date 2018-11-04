package com.boosteel.util.support;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Patterns {

    public static final String[] exec(String pattern, String target) {
        return exec(Pattern.compile(pattern), target);
    }

    public static final String[] exec(Pattern pattern, String target) {
        Matcher matcher = pattern.matcher(target);
        if (matcher.find()) {
            int count = matcher.groupCount() + 1;
            String[] values = new String[count];
            values[0] = matcher.group();
            for (int i = 1; i < count; i++) values[i] = matcher.group(i);
            return values;
        } else {
            return null;
        }
    }

    public static final void forEach(String pattern, String target, FOREACH foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH1 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH2 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH3 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH4 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH5 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH6 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH7 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH8 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH9 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH10 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH11 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH12 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH13 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH14 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH15 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH16 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH17 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH18 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH19 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH20 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH21 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(String pattern, String target, FOREACH22 foreach) throws Exception {
        forEach(Pattern.compile(pattern), target, foreach);
    }

    public static final void forEach(Pattern pattern, String target, FOREACH foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find()) foreach.accept(i++, m.group());
    }

    public static final void forEach(Pattern pattern, String target, FOREACH1 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find()) foreach.accept(i++, m.group(), m.group(1));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH2 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find()) foreach.accept(i++, m.group(), m.group(1), m.group(2));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH3 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find()) foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH4 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find()) foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH5 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find()) foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH6 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH7 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH8 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH9 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH10 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH11 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH12 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH13 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH14 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH15 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH16 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH17 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16), m.group(17));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH18 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16), m.group(17), m.group(18));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH19 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16), m.group(17), m.group(18), m.group(19));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH20 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16), m.group(17), m.group(18), m.group(19), m.group(20));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH21 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16), m.group(17), m.group(18), m.group(19), m.group(20), m.group(21));
    }

    public static final void forEach(Pattern pattern, String target, FOREACH22 foreach) throws Exception {
        Matcher m = pattern.matcher(target);
        int i = 0;
        while (m.find())
            foreach.accept(i++, m.group(), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15), m.group(16), m.group(17), m.group(18), m.group(19), m.group(20), m.group(21), m.group(22));
    }

    public interface FOREACH {
        void accept(int index, String group) throws Exception;
    }

    public interface FOREACH1 {
        void accept(int index, String group, String g1) throws Exception;
    }

    public interface FOREACH2 {
        void accept(int index, String group, String g1, String g2) throws Exception;
    }

    public interface FOREACH3 {
        void accept(int index, String group, String g1, String g2, String g3) throws Exception;
    }

    public interface FOREACH4 {
        void accept(int index, String group, String g1, String g2, String g3, String g4) throws Exception;
    }

    public interface FOREACH5 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5) throws Exception;
    }

    public interface FOREACH6 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6) throws Exception;
    }

    public interface FOREACH7 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7) throws Exception;
    }

    public interface FOREACH8 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8) throws Exception;
    }

    public interface FOREACH9 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9) throws Exception;
    }

    public interface FOREACH10 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10) throws Exception;
    }

    public interface FOREACH11 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11) throws Exception;
    }

    public interface FOREACH12 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12) throws Exception;
    }

    public interface FOREACH13 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13) throws Exception;
    }

    public interface FOREACH14 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14) throws Exception;
    }

    public interface FOREACH15 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14, String g15) throws Exception;
    }

    public interface FOREACH16 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14, String g15, String g16) throws Exception;
    }

    public interface FOREACH17 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14, String g15, String g16, String g17) throws Exception;
    }

    public interface FOREACH18 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14, String g15, String g16, String g17, String g18) throws Exception;
    }

    public interface FOREACH19 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14, String g15, String g16, String g17, String g18, String g19) throws Exception;
    }

    public interface FOREACH20 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14, String g15, String g16, String g17, String g18, String g19, String g20) throws Exception;
    }

    public interface FOREACH21 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14, String g15, String g16, String g17, String g18, String g19, String g20, String g21) throws Exception;
    }

    public interface FOREACH22 {
        void accept(int index, String group, String g1, String g2, String g3, String g4, String g5, String g6, String g7, String g8, String g9, String g10, String g11, String g12, String g13, String g14, String g15, String g16, String g17, String g18, String g19, String g20, String g21, String g22) throws Exception;
    }
}
