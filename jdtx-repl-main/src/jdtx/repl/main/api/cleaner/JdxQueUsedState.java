package jdtx.repl.main.api.cleaner;

import jandcode.web.*;
import jdtx.repl.main.api.data_serializer.*;
import org.json.simple.*;

/**
 * Информация о состоянии применения реплик на рабочей станции
 */
public class JdxQueUsedState {

    public long queInUsed = -1;
    public long queIn001Used = -1;

    public void toJson(JSONObject json) {
        json.put("queInUsed", queInUsed);
        json.put("queIn001Used", queIn001Used);
    }

    public void fromJson(JSONObject json) {
        queInUsed = UtJdxData.longValueOf(json.get("queInUsed"), -1L);
        queIn001Used = UtJdxData.longValueOf(json.get("queIn001Used"), -1L);
    }

    @Override
    public String toString() {
        JSONObject json = new JSONObject();

        toJson(json);

        return UtJson.toString(json);
    }
}
