package jdtx.repl.main.api.replica;

import org.json.simple.*;

/**
 * Информация о реплике
 */
public class ReplicaInfo {

    public long wsId;
    public long age;
    public int replicaType;
    public String crc;

    @Override
    public String toString() {
        return "{\"wsId\": " + wsId + ", \"age\": " + age + ", \"replicaType\": " + replicaType + ", \"crc\": \"" + crc + "\"}";
    }

    public static ReplicaInfo fromJSONObject(JSONObject res) {
        ReplicaInfo info = new ReplicaInfo();
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
