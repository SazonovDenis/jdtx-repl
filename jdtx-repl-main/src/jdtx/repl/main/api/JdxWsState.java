package jdtx.repl.main.api;

import jdtx.repl.main.api.data_serializer.*;
import org.json.simple.*;

public class JdxWsState {

    public Long QUE_IN_NO = null;
    public Long QUE_IN001_NO = null;
    public Long QUE_IN_NO_DONE = null;
    public Long QUE_IN001_NO_DONE = null;
    public Long QUE_OUT_NO_DONE = null;
    public Long MAIL_SEND_DONE = null;
    public Long QUE_OUT_NO = null;
    public Long AGE = null;
    public Long MUTE = null;

    public void toJson(JSONObject wsStateJson) {
        wsStateJson.put("que_in_no", QUE_IN_NO);
        wsStateJson.put("que_in001_no", QUE_IN001_NO);
        wsStateJson.put("que_in_no_done", QUE_IN_NO_DONE);
        wsStateJson.put("que_in001_no_done", QUE_IN001_NO_DONE);
        wsStateJson.put("que_out_no_done", QUE_OUT_NO_DONE);
        wsStateJson.put("mail_send_done", MAIL_SEND_DONE);
        wsStateJson.put("que_out_no", QUE_OUT_NO);
        wsStateJson.put("age", AGE);
        wsStateJson.put("mute", MUTE);
    }

    public void fromJson(JSONObject wsStateJson) {
        QUE_IN_NO = UtData.longValueOf(wsStateJson.get("que_in_no"));
        QUE_IN001_NO = UtData.longValueOf(wsStateJson.get("que_in001_no"));
        QUE_IN_NO_DONE = UtData.longValueOf(wsStateJson.get("que_in_no_done"));
        QUE_IN001_NO_DONE = UtData.longValueOf(wsStateJson.get("que_in001_no_done"));
        QUE_OUT_NO_DONE = UtData.longValueOf(wsStateJson.get("que_out_no_done"));
        MAIL_SEND_DONE = UtData.longValueOf(wsStateJson.get("mail_send_done"));
        QUE_OUT_NO = UtData.longValueOf(wsStateJson.get("que_out_no"));
        AGE = UtData.longValueOf(wsStateJson.get("age"));
        MUTE = UtData.longValueOf(wsStateJson.get("mute"));
    }

}
