package jdtx.repl.main.api.mailer;

import org.json.simple.*;

import static jdtx.repl.main.api.JdxUtils.longValueOf;

public class SendRequiredInfo {

    public long requiredFrom = -1;
    public long requiredTo = -1;
    public boolean recreate = false;

    public SendRequiredInfo() {
        super();
    }

    public SendRequiredInfo(JSONObject required) {
        super();
        requiredFrom = longValueOf(required.getOrDefault("requiredFrom", -1));
        requiredTo = longValueOf(required.getOrDefault("requiredTo", -1));
        recreate = Boolean.valueOf(String.valueOf(required.getOrDefault("recreate", false)));
    }

}
