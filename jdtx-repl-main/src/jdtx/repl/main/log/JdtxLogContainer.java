package jdtx.repl.main.log;

import java.util.*;

public class JdtxLogContainer {

    private static Map<String, String> values = new HashMap<>();

    public static synchronized void setLogValue(String key, String logValue) {
        values.put(key, logValue);
    }

    public static synchronized String getLogValue(String key) {
        return values.get(key);
    }

    public static synchronized Map<String, String> getLogValues() {
        Map<String, String> res = new HashMap<>();
        res.putAll(values);
        return res;
    }

}
