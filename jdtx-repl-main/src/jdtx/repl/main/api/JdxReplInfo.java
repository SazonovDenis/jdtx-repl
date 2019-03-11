package jdtx.repl.main.api;

/**
 * Информация о реплике
 */
public class JdxReplInfo {

    String crc;
    long wsId;
    long no;


    @Override
    public String toString() {
        return "{\"crc\": \"" + crc + "\", \"wsId\": " + wsId + ", \"no\": " + no + "}";
    }

}
