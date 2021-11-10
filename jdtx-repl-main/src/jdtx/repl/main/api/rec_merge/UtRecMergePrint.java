package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

import javax.xml.stream.*;
import java.io.*;
import java.util.*;

public class UtRecMergePrint {

    //todo все тесты по merge и relocate - выполнить и вычистить лишнее
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
            System.out.println(mergeTask.tableName + ": " + mergeTask.recordEtalon);
            System.out.println("Delete: " + mergeTask.recordsDelete);
        }
    }

    public static void printMergeResults(File fileResults) throws Exception {
        // Читаем результат выполнения задачи
        RecMergeResultReader resultReader = new RecMergeResultReader(new FileInputStream(fileResults));

        // Печатаем результат выполнения задачи
        UtRecMergePrint utRecMergePrint = new UtRecMergePrint();
        utRecMergePrint.printMergeResults(resultReader);

        //
        resultReader.close();
    }

    public void printMergeResults(RecMergeResultReader resultReader) throws Exception {
        System.out.println("MergeResults:");
        System.out.println();

        //
        MergeResultTableItem tableItem = resultReader.nextResultTable();

        while (tableItem != null) {
            String tableName = tableItem.tableName;

            if (tableItem.tableOperation == MergeOprType.UPD) {
                System.out.println("Records updated in " + tableName + ":");
            } else {
                System.out.println("Records deleted from " + tableName + ":");
            }

            //
            doRecs(resultReader);

            //
            System.out.println();


            //
            tableItem = resultReader.nextResultTable();
        }
    }

    private void doRecs(RecMergeResultReader resultReader) throws Exception {
        //
        Map<String, Object> rec = resultReader.nextRec();
        while (rec != null) {
            System.out.println(rec);

            //
            rec = resultReader.nextRec();
        }
    }


}
