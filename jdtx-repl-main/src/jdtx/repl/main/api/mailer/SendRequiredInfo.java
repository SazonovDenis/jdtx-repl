package jdtx.repl.main.api.mailer;

import jdtx.repl.main.api.data_serializer.*;
import org.json.simple.*;

public class SendRequiredInfo {

    public long requiredFrom = -1;
    public long requiredTo = -1;
    public boolean recreate = false;

    public SendRequiredInfo() {
        super();
    }

    public SendRequiredInfo(JSONObject required) {
        super();
        requiredFrom = UtJdxData.longValueOf(required.get("requiredFrom"), -1L);
        requiredTo = UtJdxData.longValueOf(required.get("requiredTo"), -1L);
        recreate = UtJdxData.booleanValueOf(required.get("recreate"), false);
    }

}
