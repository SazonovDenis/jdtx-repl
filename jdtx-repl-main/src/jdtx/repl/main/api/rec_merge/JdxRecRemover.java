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
        if (resultFile != null && resultFile.exists()) {
            throw new XError("Result file already exists: " + resultFile.getCanonicalPath());
        }

        // Для устранения проблемы UpperCase
        tableName = struct.getTable(tableName).getName();

        // Начинаем писать
        RecMergeResultWriter recMergeResultWriter = null;
        if (resultFile != null) {
            recMergeResultWriter = new RecMergeResultWriter();
            recMergeResultWriter.open(resultFile);
        }

        //
        UtRecMerger utRecMerger = new UtRecMerger(db, struct);

        //
        Map<String, Set<Long>> deletedRecordsInTables_Global = new HashMap<>();
        Set<String> deletedTablesGlobal = new HashSet<>();

        // Первая таблица в задание - та, которую передали в tableName
        Set<Long> recordsDelete = new HashSet<>();
        recordsDelete.add(tableId);

        // Прочитаем удаляемые записи и сохраним их в resultFile
        db.startTran();
        try {
            // Сохраним удаляемые из самой tableName
            if (resultFile != null) {
                utRecMerger.saveRecordsTable(tableName, recordsDelete, dataSerializer, recMergeResultWriter);
            }

            // Сохраним удаляемые данные из зависимых от tableName (слой за слоем)

            // В первый слой ссылок добавляем записи на удаление из текущей таблицы
            String levelIndent = "";
            Map<String, Set<Long>> deletedRecordsInTables = new HashMap<>();
            deletedRecordsInTables.put(tableName, recordsDelete);

            // Переходим от текущего слоя ссылок переходим к следующему, пока на очередном слое не останется сылок
            do {
                Map<String, Set<Long>> deletedRecordsInTables_Curr = new HashMap<>();

                // Собираем зависимости от таблиц из текуших deletedRecordsInTables
                Collection<String> refTables = deletedRecordsInTables.keySet();
                for (String tableNameRef : refTables) {
                    Set<Long> recordsDeleteRef = deletedRecordsInTables.get(tableNameRef);

                    // Если есть записи по ссылке, то разматываем её
                    if (recordsDeleteRef.size() > 0) {
                        //
                        Map<String, Set<Long>> deletedRecordsInTable_Ref = utRecMerger.loadRecordsRefTable(tableNameRef, recordsDeleteRef, dataSerializer, recMergeResultWriter, MergeOprType.DEL);

                        //
                        log.info(levelIndent + "Dependences for: " + tableNameRef + ", count: " + recordsDeleteRef.size() + ", ids: " + recordsDeleteRef);
                        if (deletedRecordsInTable_Ref.keySet().size() == 0) {
                            log.info(levelIndent + "  " + "<no ref>");
                        } else {
                            for (String tableNameRef_refTableName : deletedRecordsInTable_Ref.keySet()) {
                                Set<Long> deletedRecordsInTable_RefRef = deletedRecordsInTable_Ref.get(tableNameRef_refTableName);
                                log.info(levelIndent + "  " + tableNameRef_refTableName + ", count: " + deletedRecordsInTable_RefRef.size() + ", ids: " + deletedRecordsInTable_RefRef);
                            }
                        }

                        //
                        mergeMaps(deletedRecordsInTable_Ref, deletedRecordsInTables_Curr);
                    }
                }

                // Пополняем общий список зависимостей
                mergeMaps(deletedRecordsInTables_Curr, deletedRecordsInTables_Global);
                deletedTablesGlobal.addAll(refTables);

                // Теперь переходим на зависимости следующего слоя
                deletedRecordsInTables = deletedRecordsInTables_Curr;
                //
                levelIndent = levelIndent + "  ";
            } while (deletedRecordsInTables.size() != 0);

        } finally {
            db.rollback();
        }


        // Завершаем сохранение записи
        if (resultFile != null) {
            recMergeResultWriter.close();
        }


        // Выполняем удаление
        db.startTran();
        try {
            // Удаление из зависимых
            List<String> refsToTableSorted = UtJdx.getSortedKeys(struct.getTables(), deletedTablesGlobal);
            for (int i = refsToTableSorted.size() - 1; i >= 0; i--) {
                String tableNameRef = refsToTableSorted.get(i);
                Set<Long> recordsDeleteRef = deletedRecordsInTables_Global.get(tableNameRef);
                if (recordsDeleteRef.size() != 0) {
                    log.info("delete from: " + tableNameRef + ", count: " + recordsDeleteRef.size() + ", ids: " + recordsDeleteRef);
                    utRecMerger.execRecordsDelete(tableNameRef, recordsDeleteRef);
                } else {
                    log.info("delete from: " + tableNameRef + " <no records>");
                }
            }

            // Удаление из основной таблицы
            log.info("delete main: " + tableName + ", count: " + recordsDelete.size() + ", ids: " + recordsDelete);
            utRecMerger.execRecordsDelete(tableName, recordsDelete);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }

    /**
     * Сливает две мапы, содержащие списки, таким образом,
     * что уже имеющиеся значения в списках не затираются, а объединяются
     *
     * @param mapSource Map с добавляемыми списками
     * @param mapDest   пополняемая Map со списками
     */
    private void mergeMaps(Map<String, Set<Long>> mapSource, Map<String, Set<Long>> mapDest) {
        for (String key : mapSource.keySet()) {
            Set<Long> destList = mapDest.get(key);
            Set<Long> addList = mapSource.get(key);
            if (destList == null) {
                mapDest.put(key, addList);
            } else {
                destList.addAll(addList);
            }
        }
    }


}
