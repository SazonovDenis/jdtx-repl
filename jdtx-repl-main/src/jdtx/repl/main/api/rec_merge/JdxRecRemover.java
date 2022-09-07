package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 * Умеет каскадно удалять записи.
 */
public class JdxRecRemover {

    Db db;
    JdxDbUtils dbu;
    IJdxDbStruct struct;
    IJdxDataSerializer dataSerializer;

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxRecRemover");

    //
    public JdxRecRemover(Db db, IJdxDbStruct struct, IJdxDataSerializer dataSerializer) throws Exception {
        this.db = db;
        this.struct = struct;
        this.dbu = new JdxDbUtils(db, struct);
        this.dataSerializer = dataSerializer;
    }

    public void removeRecCascade(String tableName, long tableId, File resultFile) throws Exception {
        if (tableId == 0) {
            throw new XError("Error delete: table.id == 0");
        }

        // Не затирать существующий
        if (resultFile.exists()) {
            throw new XError("Result file already exists: " + resultFile.getCanonicalPath());
        }

        // Для устранения проблемы UpperCase
        tableName = struct.getTable(tableName).getName();

        // Начинаем писать
        RecMergeResultWriter recMergeResultWriter = new RecMergeResultWriter();
        recMergeResultWriter.open(resultFile);

        //
        UtRecMerger utRecMerger = new UtRecMerger(db, struct);

        //
        Map<String, Set<Long>> deletedRecordsInTablesGlobal = new HashMap<>();
        Set<String> deletedTablesGlobal = new HashSet<>();

        // Первая таблица в задание - та, которую передали в tableName
        Set<Long> recordsDelete = new HashSet<>();
        recordsDelete.add(tableId);

        // Сохраним удаляемые
        db.startTran();
        try {
            // Сохраним удаляемые из самой tableName
            utRecMerger.saveRecordsTable(tableName, recordsDelete, recMergeResultWriter, dataSerializer);

            // Сохраним удаляемые данные из зависимых от таблицы tableName (слой за слоем)
            Map<String, Set<Long>> deletedRecordsInTables = new HashMap<>();
            deletedRecordsInTables.put(tableName, recordsDelete);
            do {
                Map<String, Set<Long>> deletedRecordsInTablesCurr = new HashMap<>();

                // Собираем зависимости от таблиц из текуших deletedRecordsInTables
                Collection<String> refTables = deletedRecordsInTables.keySet();
                for (String tableNameRef : refTables) {
                    Set<Long> recordsDeleteRef = deletedRecordsInTables.get(tableNameRef);
                    Map<String, Set<Long>> deletedRecordsInTablesRef = utRecMerger.saveRecordsRefTable(tableNameRef, recordsDeleteRef, recMergeResultWriter, MergeOprType.DEL, dataSerializer);

                    //
                    log.info("dependences for " + tableNameRef + ": " + deletedRecordsInTablesRef.keySet());

                    //
                    addAllListValuesInMaps(deletedRecordsInTablesCurr, deletedRecordsInTablesRef);
                }

                // Пополняем общий список зависимостей
                addAllListValuesInMaps(deletedRecordsInTablesGlobal, deletedRecordsInTablesCurr);
                deletedTablesGlobal.addAll(refTables);

                // Теперь переходим на зависимости следующего слоя
                deletedRecordsInTables = deletedRecordsInTablesCurr;
            } while (deletedRecordsInTables.size() != 0);

        } finally {
            db.rollback();
        }


        // Завершаем писать
        recMergeResultWriter.close();


        // Выполняем удаление
        db.startTran();
        try {
            // Удаление из зависимых
            List<String> refsToTableSorted = UtJdx.getSortedKeys(struct.getTables(), deletedTablesGlobal);
            for (int i = refsToTableSorted.size() - 1; i > 0; i--) {
                String tableNameRef = refsToTableSorted.get(i);
                Set<Long> recordsDeleteRef = deletedRecordsInTablesGlobal.get(tableNameRef);
                log.info("delete from: " + tableNameRef + " (count: " + recordsDeleteRef.size() + ")");
                utRecMerger.execRecordsDelete(tableNameRef, recordsDeleteRef);
            }

            // Удаление из основной таблицы
            log.info("delete from: " + tableName + " (count: " + recordsDelete.size() + ")");
            utRecMerger.execRecordsDelete(tableName, recordsDelete);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }

    /**
     * Сливает два мапы, содержащие списки, таким образим, что уже имеющиеся значения в списке не затираются, а объединяются
     *
     * @param mapDest пополняемая Map со списками
     * @param mapAdd  Map с добавляемыми списками
     */
    private void addAllListValuesInMaps(Map<String, Set<Long>> mapDest, Map<String, Set<Long>> mapAdd) {
        for (String key : mapAdd.keySet()) {
            Set<Long> destList = mapDest.get(key);
            Set<Long> addList = mapAdd.get(key);
            if (destList == null) {
                mapDest.put(key, addList);
            } else {
                destList.addAll(addList);
            }
        }
    }


}
