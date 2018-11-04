package com.boosteel.util.support;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class IO {

    public static final String read(InputStream is, String charset) throws Exception {
        StringBuffer buf = new StringBuffer();
        read(is, charset, (line, i) -> buf.append(line));
        return buf.toString();
    }
    public static final List<String> realLines(InputStream is, String charset) throws Exception {
        List<String> lines = new ArrayList<>();
        read(is, charset, (line, i) -> lines.add(line));
        return lines;
    }
    public static final void read(InputStream is, String charset, Reader lambda) throws Exception {
        try(BufferedReader br = reader(is, charset)) {
            String line = null;
            int index = 0;
            while((line = br.readLine()) != null)
                lambda.accept(line, index++);
        } catch (IOException e) {
            /*
             * 'Premature EOF'는 끝문자가 제대로 전달되지 않아서 오는 에러다.
             * EOF 에러는 무시해버린다.
             */
            if(e.getMessage().contains("EOF")) return;
            throw e;
        }
    }

    public static final BufferedReader reader(InputStream is, String charset) throws IOException {
        return new BufferedReader(new InputStreamReader(is, charset));
    }

    interface Reader {
        void accept(String line, int index) throws Exception;
    }
}
