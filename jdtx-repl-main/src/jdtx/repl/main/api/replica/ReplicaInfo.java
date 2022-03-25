package jdtx.repl.main.api.replica;

import jdtx.repl.main.api.data_serializer.*;
import org.joda.time.*;
import org.json.simple.*;

/**
 * Информация о реплике
 */
public class ReplicaInfo implements IReplicaInfo {


    private long wsId;
    private long no;
    private long age;
    private String crc;
    private DateTime dtFrom;
    private DateTime dtTo;
    private int replicaType;
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

    public long getNo() {
        return no;
    }

    public void setNo(long no) {
        this.no = no;
    }

    public String getCrc() {
        return crc;
    }

    public void setCrc(String crc) {
        this.crc = crc;
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

    public String getDbStructCrc() {
        return dbStructCrc;
    }

    public void setDbStructCrc(String crc) {
        this.dbStructCrc = crc;
    }

    public void assign(IReplicaInfo src) {
        this.wsId = src.getWsId();
        this.no = src.getNo();
        this.age = src.getAge();
        this.crc = src.getCrc();
        this.dtFrom = src.getDtFrom();
        this.dtTo = src.getDtTo();
        this.replicaType = src.getReplicaType();
        this.dbStructCrc = src.getDbStructCrc();
    }

    public void fromJSONObject(JSONObject infoJson) {
        this.wsId = UtJdxData.longValueOf(infoJson.get("wsId"), -1L);
        this.no = UtJdxData.longValueOf(infoJson.get("no"), -1L);
        this.age = UtJdxData.longValueOf(infoJson.get("age"), -1L);
        this.crc = (String) infoJson.get("crc");
        this.dtFrom = UtJdxData.dateTimeValueOf((String) infoJson.get("dtFrom"));
        this.dtTo = UtJdxData.dateTimeValueOf((String) infoJson.get("dtTo"));
        this.replicaType = UtJdxData.intValueOf(infoJson.get("replicaType"));
        this.dbStructCrc = (String) infoJson.get("dbStructCrc");
    }

    public JSONObject toJSONObject() {
        JSONObject res = new JSONObject();
        //
        res.put("wsId", wsId);
        res.put("no", no);
        res.put("age", age);
        res.put("crc", crc);
        if (dtFrom != null) {
            res.put("dtFrom", String.valueOf(dtFrom));
        }
        if (dtTo != null) {
            res.put("dtTo", String.valueOf(dtTo));
        }
        res.put("replicaType", replicaType);
        //
        return res;
    }

    public String toJSONString() {
        return "{" +
                "\"wsId\": " + wsId + "," +
                "\"no\": " + no + "," +
                "\"age\": " + age + "," +
                "\"crc\": \"" + crc + "\"," +
                "\"replicaType\": " + replicaType + "," +
                "\"dbStructCrc\": \"" + dbStructCrc + "\"," +
                "\"dtFrom\": \"" + dtFrom + "\"," +
                "\"dtTo\": \"" + dtTo + "\"" +
                "}";
    }

}
