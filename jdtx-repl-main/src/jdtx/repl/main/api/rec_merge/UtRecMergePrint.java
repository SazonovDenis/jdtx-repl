package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

import java.util.*;

public class UtRecMergePrint {

    public static void printRecordsUpdated(RecordsUpdatedMap recordsUpdatedMap) {
        for (String key : recordsUpdatedMap.keySet()) {
            RecordsUpdated recordsUpdated = recordsUpdatedMap.get(key);
            System.out.println("ref: " + recordsUpdated.refTableName + "." + recordsUpdated.refFieldName);
            if (recordsUpdated.recordsUpdated == null || recordsUpdated.recordsUpdated.size() == 0) {
                System.out.println("Ref records updated: empty");
            } else {
                UtData.outTable(recordsUpdated.recordsUpdated);
            }
            System.out.println();
        }
    }


    public static void printTasks(Collection<RecMergePlan> mergeTasks) {
        System.out.println("MergeTasks count: " + mergeTasks.size());
        System.out.println();
        for (RecMergePlan mergeTask : mergeTasks) {
            System.out.println("Table: " + mergeTask.tableName);
            System.out.println("Etalon: " + mergeTask.recordEtalon);
            System.out.println("Delete: " + mergeTask.recordsDelete);
            System.out.println();
        }
    }

    public static void printMergeResults(MergeResultTableMap mergeResults) {
        System.out.println("MergeResults:");
        System.out.println();
        for (String taskTableName : mergeResults.keySet()) {
            System.out.println("TableName: " + taskTableName);
            System.out.println();

            MergeResultTable mergeResultTable = mergeResults.get(taskTableName);

            System.out.println("Records updated for tables, referenced to " + taskTableName + ":");
            printRecordsUpdated(mergeResultTable.recordsUpdated);

            System.out.println("Records deleted from " + taskTableName + ":");
            if (mergeResultTable.recordsDeleted == null || mergeResultTable.recordsDeleted.size() == 0) {
                System.out.println("Records deleted: empty");
            } else {
                UtData.outTable(mergeResultTable.recordsDeleted);
            }
        }
    }


}
