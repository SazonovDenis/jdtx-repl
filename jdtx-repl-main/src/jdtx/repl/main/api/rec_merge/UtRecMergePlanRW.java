package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

/**
 * Чтение/запись RecMergePlan в JSON
 */
public class UtRecMergePlanRW {


    public Collection<RecMergePlan> readPlans(String fileName) throws Exception {
        Collection<RecMergePlan> mergePlans;

        InputStream inputStream = new FileInputStream(fileName);
        try {
            UtRecMergePlanRW reader = new UtRecMergePlanRW();
            mergePlans = reader.readPlans(inputStream);
        } finally {
            inputStream.close();
        }

        //
        return mergePlans;
    }

    public Collection<RecMergePlan> readPlans(InputStream inputStream) throws Exception {
        Collection<RecMergePlan> mergePlans = new ArrayList<>();

        Reader reader = new InputStreamReader(inputStream);
        JSONParser parser = new JSONParser();
        JSONArray jsonPoints = (JSONArray) parser.parse(reader);
        reader.close();

        //
        for (Object k : jsonPoints) {
            JSONObject planJson = (JSONObject) k;
            RecMergePlan plan = new RecMergePlan();
            plan.tableName = planJson.get("tableName").toString();
            plan.recordEtalon.putAll((Map) planJson.get("recordEtalon"));
            plan.recordsDelete.addAll((Collection<String>) planJson.get("recordsDelete"));
            mergePlans.add(plan);
        }

        //
        return mergePlans;
    }

    public void writePlans(Collection<RecMergePlan> mergePlans, String fileName) throws Exception {
        JSONArray json = new JSONArray();
        for (RecMergePlan plan : mergePlans) {
            JSONObject planJson = new JSONObject();
            planJson.put("tableName", plan.tableName);
            planJson.put("recordEtalon", plan.recordEtalon);
            planJson.put("recordsDelete", plan.recordsDelete);
            json.add(planJson);
        }
        UtFile.saveString(json.toJSONString(), new File(fileName));
    }

    public void writeDuplicates(Collection<RecDuplicate> recDuplicates, String fileName) throws Exception {
        JSONArray json = new JSONArray();
        for (RecDuplicate duplicate : recDuplicates) {
            JSONObject planJson = new JSONObject();
            planJson.put("params", duplicate.params);
            planJson.put("records", dataStoreToList(duplicate.records));
            json.add(planJson);
        }
        UtFile.saveString(json.toJSONString(), new File(fileName));
    }

    private List<Map> dataStoreToList(DataStore store) {
        List<Map> res = new ArrayList<>();
        if (store != null) {
            for (DataRecord rec : store) {
                res.add(rec.getValues());
            }
        }
        return res;
    }

}
