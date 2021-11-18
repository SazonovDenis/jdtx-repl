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
        requiredFrom = UtData.longValueOf(required.get("requiredFrom"), -1L);
        requiredTo = UtData.longValueOf(required.get("requiredTo"), -1L);
        recreate = UtData.booleanValueOf(required.get("recreate"), false);
    }

}
