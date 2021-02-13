package jdtx.repl.main.api.replica;

import org.joda.time.*;
import org.json.simple.*;

/**
 * Информация о реплике
 */
public class ReplicaInfo implements IReplicaInfo {


    private long wsId;
    private long age;
    private DateTime dtFrom;
    private DateTime dtTo;
    private int replicaType;
    private String crc;
    private String dbStructCrc;

    public long getWsId() {
        return wsId;
    }

    public void setWsId(long wsId) {
        this.wsId = wsId;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public DateTime getDtFrom() {
        return dtFrom;
    }

    public void setDtFrom(DateTime dtFrom) {
        this.dtFrom = dtFrom;
    }

    public DateTime getDtTo() {
        return dtTo;
    }

    public void setDtTo(DateTime dtTo) {
        this.dtTo = dtTo;
    }

    public int getReplicaType() {
        return replicaType;
    }

    public void setReplicaType(int replicaType) {
        this.replicaType = replicaType;
    }

    public String getCrc() {
        return crc;
    }

    public void setCrc(String crc) {
        this.crc = crc;
    }

    public String getDbStructCrc() {
        return dbStructCrc;
    }

    public void setDbStructCrc(String crc) {
        this.dbStructCrc = crc;
    }

    public void assign(IReplicaInfo src) {
        this.wsId = src.getWsId();
        this.age = src.getAge();
        this.dtFrom = src.getDtFrom();
        this.dtTo = src.getDtTo();
        this.replicaType = src.getReplicaType();
        this.crc = src.getCrc();
        this.dbStructCrc = src.getDbStructCrc();
    }

    @Override
    public String toString() {
        return "{\"wsId\": " + wsId + ", \"age\": " + age + ", \"replicaType\": " + replicaType + ", \"crc\": \"" + crc + "\", \"dbStructCrc\": \"" + dbStructCrc + "\", \"dtFrom\": \"" + dtFrom + "\", \"dtTo\": \"" + dtTo + "\"}";
    }

    public static ReplicaInfo fromJSONObject(JSONObject res) {
        ReplicaInfo info = new ReplicaInfo();
        info.wsId = (long) res.get("wsId");
        info.age = (long) res.get("age");
        String dtFrom = (String) res.get("dtFrom");
        if (dtFrom != null && dtFrom.compareTo("null") != 0) {
            info.dtFrom = new DateTime(dtFrom);
        }
        String dtTo = (String) res.get("dtTo");
        if (dtTo != null && dtTo.compareTo("null") != 0) {
            info.dtTo = new DateTime(dtTo);
        }
        info.replicaType = Integer.valueOf(String.valueOf(res.get("replicaType")));  // так сложно - потому что в res.get("replicaType") оказывается Long
        info.crc = (String) res.get("crc");
        info.dbStructCrc = (String) res.get("dbStructCrc");
        return info;
    }

    public JSONObject toJSONObject() {
        JSONObject res = new JSONObject();
        res.put("wsId", wsId);
        res.put("age", age);
        if (dtFrom != null) {
            res.put("dtFrom", String.valueOf(dtFrom));
        }
        if (dtTo != null) {
            res.put("dtTo", dtTo.toString());
        }
        res.put("replicaType", replicaType);
        res.put("crc", crc);
        return res;
    }

}
