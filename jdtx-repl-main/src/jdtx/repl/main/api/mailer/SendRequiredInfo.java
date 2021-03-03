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
        requiredFrom = UtJdx.longValueOf(required.getOrDefault("requiredFrom", -1));
        requiredTo = UtJdx.longValueOf(required.getOrDefault("requiredTo", -1));
        recreate = Boolean.valueOf(String.valueOf(required.getOrDefault("recreate", false)));
    }

}
