package jdtx.repl.main.api.mailer;

import jdtx.repl.main.api.data_serializer.*;
import org.json.simple.*;

public class RequiredInfo {

    public long requiredFrom = -1;
    public long requiredTo = -1;
    public boolean recreate = false;
    public String executor = null;

    public static String EXECUTOR_WS = "ws";
    public static String EXECUTOR_SRV = "srv";

    public RequiredInfo() {
        super();
    }

    public RequiredInfo(JSONObject required) {
        super();
        requiredFrom = UtJdxData.longValueOf(required.get("requiredFrom"), -1L);
        requiredTo = UtJdxData.longValueOf(required.get("requiredTo"), -1L);
        recreate = UtJdxData.booleanValueOf(required.get("recreate"), false);
        executor = UtJdxData.stringValueOf(required.get("executor"), null);
    }

    public void assign(RequiredInfo requiredInfo) {
        this.requiredFrom = requiredInfo.requiredFrom;
        this.requiredTo = requiredInfo.requiredTo;
        this.recreate = requiredInfo.recreate;
        this.executor = requiredInfo.executor;
    }

    @Override
    public String toString() {
        if (requiredFrom == -1) {
            return "no required";
        } else if (requiredTo > 0) {
            return "required: " + requiredFrom + " .. " + requiredTo + ", recreate: " + recreate + ", executor: " + executor;
        } else {
            return "required: " + requiredFrom + " .. " + "all" + ", recreate: " + recreate + ", executor: " + executor;
        }
    }

}
