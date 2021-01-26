package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

public class UtRecMergeReader {

    public Collection<RecMergeTask> readTasks(String fileName) throws Exception {
        Collection<RecMergeTask> mergeTasks = new ArrayList<>();

        InputStream inputStream = new FileInputStream(fileName);
        Reader reader = new InputStreamReader(inputStream);
        JSONParser parser = new JSONParser();
        JSONArray jsonPoints = (JSONArray) parser.parse(reader);

        //
        for (Object k : jsonPoints) {
            JSONObject taskJson = (JSONObject) k;
            RecMergeTask task = new RecMergeTask();
            task.tableName = taskJson.get("tableName").toString();
            task.recordEtalon = new HashMap((Map) taskJson.get("recordEtalon"));
            task.recordsDelete = new ArrayList<>((Collection<Long>) taskJson.get("recordsDelete"));
            mergeTasks.add(task);
        }


        return mergeTasks;
    }

    public void writeTasks(Collection<RecMergeTask> mergeTasks, String fileName) throws Exception {
        JSONArray json = new JSONArray();
        for (RecMergeTask task : mergeTasks) {
            JSONObject taskJson = new JSONObject();
            taskJson.put("tableName", task.tableName);
            taskJson.put("recordEtalon", task.recordEtalon);
            taskJson.put("recordsDelete", task.recordsDelete);
            json.add(taskJson);
        }
        UtFile.saveString(json.toJSONString(), new File(fileName));
    }

    public void writeResilts(Map<String, MergeResultTable> mergeResults, String fileName) throws Exception {
        JSONObject json = new JSONObject();
        for (String tableName : mergeResults.keySet()) {
            MergeResultTable mergeResult = mergeResults.get(tableName);

            JSONObject resultJson = new JSONObject();

            // mergeResult.recordsUpdated
            JSONObject mergeResultsRefTable = new JSONObject();
            for (String refTableName : mergeResult.recordsUpdated.keySet()) {
                MergeResultRecordsUpdated mergeResultRecordsUpdated = mergeResult.recordsUpdated.get(refTableName);
                JSONObject mergeResultRefTableJson = new JSONObject();
                //mergeResultRefTableJson.put("tableName", mergeResultRecordsUpdated.tableName);
                mergeResultRefTableJson.put("refTableName", refTableName);
                mergeResultRefTableJson.put("refFieldName", mergeResultRecordsUpdated.refFieldName);
                mergeResultRefTableJson.put("recordsUpdated", dataStoreToList(mergeResultRecordsUpdated.recordsUpdated));
                mergeResultsRefTable.put(refTableName, mergeResultRefTableJson);
            }
            resultJson.put("recordsUpdated", mergeResultsRefTable);

            // mergeResult.recordsDeleted
            List<Map> recordsDeleted = dataStoreToList(mergeResult.recordsDeleted);
            resultJson.put("recordsDeleted", recordsDeleted);

            //
            json.put(tableName, resultJson);
        }
        UtFile.saveString(json.toJSONString(), new File(fileName));
    }

    public Map<String, MergeResultTable> readResults(String fileName) throws Exception {
        return null;
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
