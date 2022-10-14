package jdtx.repl.main.api.cleaner;

import jandcode.web.*;
import jdtx.repl.main.api.data_serializer.*;
import org.json.simple.*;

public class JdxQueUsedState {

    long queOutUsed = -1;
    long queInUsed = -1;
    long queIn001Used = -1;

    public void toJson(JSONObject json) {
        json.put("out", queOutUsed);
        json.put("in", queInUsed);
        json.put("in001", queIn001Used);
    }

    public void fromJson(JSONObject json) {
        queOutUsed = UtJdxData.longValueOf(json.get("out"));
        queInUsed = UtJdxData.longValueOf(json.get("in"));
        queIn001Used = UtJdxData.longValueOf(json.get("in001"));
    }

    @Override
    public String toString() {
        JSONObject json = new JSONObject();

        toJson(json);

        return UtJson.toString(json);
    }
}
