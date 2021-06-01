package jdtx.repl.main.api.replica;

import org.json.simple.*;

public class ReplicaFileInfo extends ReplicaInfo implements IReplicaFileInfo {

    private String crc;

    public String getCrc() {
        return crc;
    }

    public void setCrc(String crc) {
        this.crc = crc;
    }

    public void fromJSONObject(JSONObject infoJson) {
        super.fromJSONObject(infoJson);
        this.crc = (String) infoJson.get("crc");
    }

    public JSONObject toJSONObject() {
        JSONObject res = super.toJSONObject();
        //
        res.put("crc", crc);
        //
        return res;
    }

}
