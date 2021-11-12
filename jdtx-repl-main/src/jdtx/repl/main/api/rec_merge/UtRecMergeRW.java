package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

/**
 * Чтение/запись RecMergePlan в json
 */
public class UtRecMergeRW {


    public Collection<RecMergePlan> readTasks(String fileName) throws Exception {
        Collection<RecMergePlan> mergeTasks;

        InputStream inputStream = new FileInputStream(fileName);
        try {
            UtRecMergeRW reader = new UtRecMergeRW();
            mergeTasks = reader.readTasks(inputStream);
        } finally {
            inputStream.close();
        }

        //
        return mergeTasks;
    }

    public Collection<RecMergePlan> readTasks(InputStream inputStream) throws Exception {
        Collection<RecMergePlan> mergeTasks = new ArrayList<>();

        Reader reader = new InputStreamReader(inputStream);
        JSONParser parser = new JSONParser();
        JSONArray jsonPoints = (JSONArray) parser.parse(reader);
        reader.close();

        //
        for (Object k : jsonPoints) {
            JSONObject taskJson = (JSONObject) k;
            RecMergePlan task = new RecMergePlan();
            task.tableName = taskJson.get("tableName").toString();
            task.recordEtalon.putAll((Map) taskJson.get("recordEtalon"));
            task.recordsDelete.addAll((Collection<Long>) taskJson.get("recordsDelete"));
            mergeTasks.add(task);
        }

        //
        return mergeTasks;
    }

    public void writeTasks(Collection<RecMergePlan> mergeTasks, String fileName) throws Exception {
        JSONArray json = new JSONArray();
        for (RecMergePlan task : mergeTasks) {
            JSONObject taskJson = new JSONObject();
            taskJson.put("tableName", task.tableName);
            taskJson.put("recordEtalon", task.recordEtalon);
            taskJson.put("recordsDelete", task.recordsDelete);
            json.add(taskJson);
        }
        UtFile.saveString(json.toJSONString(), new File(fileName));
    }

    public void writeDuplicates(Collection<RecDuplicate> recDuplicates, String fileName) throws Exception {
        JSONArray json = new JSONArray();
        for (RecDuplicate duplicate : recDuplicates) {
            JSONObject taskJson = new JSONObject();
            taskJson.put("params", duplicate.params);
            taskJson.put("records", dataStoreToList(duplicate.records));
            json.add(taskJson);
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
