package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

import javax.xml.stream.*;
import java.io.*;
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


    public static void printPlans(Collection<RecMergePlan> mergePlans) {
        System.out.println("MergePlans count: " + mergePlans.size());
        System.out.println();
        for (RecMergePlan mergePlan : mergePlans) {
            System.out.println(mergePlan.tableName + ": " + mergePlan.recordEtalon);
            System.out.println("Delete: " + mergePlan.recordsDelete);
        }
    }

    public static void printDuplicates(Collection<RecDuplicate> duplicates) {
        System.out.println("Заданий на слияние: " + duplicates.size());
        for (RecDuplicate res : duplicates) {
            System.out.println("  искали: " + res.params);
            System.out.println("  нашли дубликатов: " + res.records.size());
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
        Map<String, String> rec = resultReader.nextRec();
        while (rec != null) {
            System.out.println(rec);

            //
            rec = resultReader.nextRec();
        }
    }


}
