package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

public class UtRecMergeRW {

    public Collection<RecMergePlan> readTasks(String fileName) throws Exception {
        Collection<RecMergePlan> mergeTasks = new ArrayList<>();

        InputStream inputStream = new FileInputStream(fileName);
        Reader reader = new InputStreamReader(inputStream);
        JSONParser parser = new JSONParser();
        JSONArray jsonPoints = (JSONArray) parser.parse(reader);
        reader.close();

        //
        for (Object k : jsonPoints) {
            JSONObject taskJson = (JSONObject) k;
            RecMergePlan task = new RecMergePlan();
            task.tableName = taskJson.get("tableName").toString();
            task.recordEtalon = new HashMap((Map) taskJson.get("recordEtalon"));
            task.recordsDelete = new ArrayList<>((Collection<Long>) taskJson.get("recordsDelete"));
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

    public void writeMergeResilts(MergeResultTableMap mergeResults, String fileName) throws Exception {
        JSONObject json = new JSONObject();
        for (String tableName : mergeResults.keySet()) {
            MergeResultTable mergeResult = mergeResults.get(tableName);

            JSONObject resultJson = new JSONObject();

            // mergeResult.recordsUpdated
            JSONObject mergeResultsRefTable = new JSONObject();
            for (String refTableName : mergeResult.recordsUpdated.keySet()) {
                RecordsUpdated recordsUpdated = mergeResult.recordsUpdated.get(refTableName);
                JSONObject mergeResultRefTableJson = new JSONObject();
                mergeResultRefTableJson.put("refTableName", recordsUpdated.refTableName);
                mergeResultRefTableJson.put("refFieldName", recordsUpdated.refFieldName);
                mergeResultRefTableJson.put("recordsUpdated", dataStoreToList(recordsUpdated.recordsUpdated));
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

    public MergeResultTableMap readResults(String fileName) throws Exception {
        // todo: реализовать
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
