package jdtx.repl.main.log;

public class JdtxLogStorage {

    private static String value;

    public static synchronized void setLogValue(String logValue) {
        value = logValue;
    }

    public static synchronized String getLogValue() {
        return value;
    }

}
