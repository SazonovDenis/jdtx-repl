package jdtx.repl.main.api.replica;

import org.joda.time.*;
import org.json.simple.*;

/**
 * Информация о реплике
 */
public class ReplicaInfo {

    public long wsId;
    public long age;
    public DateTime dtFrom;
    public DateTime dtTo;
    public int replicaType;
    public String crc;

    @Override
    public String toString() {
        return "{\"wsId\": " + wsId + ", \"age\": " + age + ", \"replicaType\": " + replicaType + ", \"crc\": \"" + crc + "\", \"dtFrom\": \"" + dtFrom + "\", \"dtFrom\": \"" + dtTo + "\"}";
    }

    public static ReplicaInfo fromJSONObject(JSONObject res) {
        ReplicaInfo info = new ReplicaInfo();
        info.wsId = (long) res.get("wsId");
        info.age = (long) res.get("age");
        info.dtFrom = (DateTime) res.get("dtFrom");
        info.dtTo = (DateTime) res.get("dtTo");
        info.replicaType = Integer.valueOf(String.valueOf(res.get("replicaType")));  // так сложно - потому что в res.get("replicaType") оказывается Long
        info.crc = (String) res.get("crc");
        return info;
    }

    public JSONObject toJSONObject() {
        JSONObject res = new JSONObject();
        res.put("wsId", wsId);
        res.put("age", age);
        res.put("dtFrom", dtFrom);
        res.put("dtTo", dtTo);
        res.put("replicaType", replicaType);
        res.put("crc", crc);
        return res;
    }

}
