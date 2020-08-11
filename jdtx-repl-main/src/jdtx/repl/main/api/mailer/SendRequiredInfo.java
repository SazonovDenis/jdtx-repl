package jdtx.repl.main.api.mailer;

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
        requiredFrom = Long.valueOf(String.valueOf(required.getOrDefault("requiredFrom", -1)));
        requiredTo = Long.valueOf(String.valueOf(required.getOrDefault("requiredTo", -1)));
        recreate = Boolean.valueOf(String.valueOf(required.getOrDefault("recreate", false)));
    }

}
