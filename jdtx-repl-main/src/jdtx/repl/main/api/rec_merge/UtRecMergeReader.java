package jdtx.repl.main.api.rec_merge;

import jandcode.utils.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

public class UtRecMergeReader {

    public Collection<RecMergeTask> readFromFile(String fileName) throws Exception {
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
            task.recordEtalon = new HashMap ((Map) taskJson.get("recordEtalon"));
            task.recordsDelete = new ArrayList<>((Collection<Long>) taskJson.get("recordsDelete"));
            mergeTasks.add(task);
        }


        return mergeTasks;
    }

    public void writeToFile(Collection<RecMergeTask> mergeTasks, String fileName) throws Exception {
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

}
