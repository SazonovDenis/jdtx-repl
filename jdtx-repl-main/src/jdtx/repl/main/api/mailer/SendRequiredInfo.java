package jdtx.repl.main.api.mailer;

import jdtx.repl.main.api.*;
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
        requiredFrom = UtJdx.longValueOf(required.get("requiredFrom"), -1L);
        requiredTo = UtJdx.longValueOf(required.get("requiredTo"), -1L);
        recreate = UtJdx.booleanValueOf(required.get("recreate"), false);
    }

}
