package jdtx.repl.main.api;

import jdtx.repl.main.api.data_serializer.*;
import org.json.simple.*;

public class JdxWsState {

    public Long AGE = null;
    public Long QUE_IN_NO = null;
    public Long QUE_IN001_NO = null;
    public Long QUE_OUT_NO = null;
    public Long QUE_IN_NO_DONE = null;
    public Long QUE_IN001_NO_DONE = null;
    public Long AUDIT_AGE_DONE = null;
    public Long MAIL_SEND_DONE = null;
    public Long MUTE = null;

    public void toJson(JSONObject wsStateJson) {
        wsStateJson.put("age", AGE);
        wsStateJson.put("que_in_no", QUE_IN_NO);
        wsStateJson.put("que_in001_no", QUE_IN001_NO);
        wsStateJson.put("que_out_no", QUE_OUT_NO);
        wsStateJson.put("que_in_no_done", QUE_IN_NO_DONE);
        wsStateJson.put("que_in001_no_done", QUE_IN001_NO_DONE);
        wsStateJson.put("audit_age_done", AUDIT_AGE_DONE);
        wsStateJson.put("mail_send_done", MAIL_SEND_DONE);
        wsStateJson.put("mute", MUTE);
    }

    public void fromJson(JSONObject wsStateJson) {
        AGE = UtJdxData.longValueOf(wsStateJson.get("age"));
        QUE_IN_NO = UtJdxData.longValueOf(wsStateJson.get("que_in_no"));
        QUE_IN001_NO = UtJdxData.longValueOf(wsStateJson.get("que_in001_no"));
        QUE_OUT_NO = UtJdxData.longValueOf(wsStateJson.get("que_out_no"));
        QUE_IN_NO_DONE = UtJdxData.longValueOf(wsStateJson.get("que_in_no_done"));
        QUE_IN001_NO_DONE = UtJdxData.longValueOf(wsStateJson.get("que_in001_no_done"));
        AUDIT_AGE_DONE = UtJdxData.longValueOf(wsStateJson.get("audit_age_done"));
        MAIL_SEND_DONE = UtJdxData.longValueOf(wsStateJson.get("mail_send_done"));
        MUTE = UtJdxData.longValueOf(wsStateJson.get("mute"));
    }

}
