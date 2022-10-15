package jdtx.repl.main.api.cleaner;

import jandcode.web.*;
import jdtx.repl.main.api.data_serializer.*;
import org.json.simple.*;

import java.util.*;

/**
 * Что можно уже удалять на рабочей станции (ненужный аудит и обработанные реплики)
 */
public class JdxQueCleanTask {

    public long queOutNo = -1;
    public long queInNo = -1;
    public long queIn001No = -1;

    public void toJson(JSONObject json) {
        json.put("queOutNo", queOutNo);
        json.put("queInNo", queInNo);
        json.put("queIn001No", queIn001No);
    }

    public void toMap(Map<String, Long> data) {
        data.put("queOutNo", queOutNo);
        data.put("queInNo", queInNo);
        data.put("queIn001No", queIn001No);
    }

    public void fromJson(JSONObject json) {
        queOutNo = UtJdxData.longValueOf(json.get("queOutNo"), -1L);
        queInNo = UtJdxData.longValueOf(json.get("queInNo"), -1L);
        queIn001No = UtJdxData.longValueOf(json.get("queIn001No"), -1L);
    }

    @Override
    public String toString() {
        JSONObject json = new JSONObject();

        toJson(json);

        return UtJson.toString(json);
    }
}
