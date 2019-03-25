package jdtx.repl.main.api;

import org.json.simple.*;

/**
 * Информация о реплике
 */
public class JdxReplInfo {

    long wsId;
    long age;
    int replicaType;
    String crc;

    @Override
    public String toString() {
        return "{\"wsId\": " + wsId + ", \"age\": " + age + ", \"replicaType\": " + replicaType + ", \"crc\": \"" + crc + "\"}";
    }

    public static JdxReplInfo fromJSONObject(JSONObject res) {
        JdxReplInfo info = new JdxReplInfo();
        info.crc = (String) res.get("crc");
        info.wsId = (long) res.get("wsId");
        info.replicaType = Integer.valueOf(String.valueOf(res.get("replicaType")));  // так сложно - потому что в res.get("replicaType") оказывается Long
        info.age = (long) res.get("age");
        return info;
    }

    public JSONObject toJSONObject() {
        JSONObject res = new JSONObject();
        res.put("wsId", wsId);
        res.put("age", age);
        res.put("replicaType", replicaType);
        res.put("crc", crc);
        return res;
    }

}
