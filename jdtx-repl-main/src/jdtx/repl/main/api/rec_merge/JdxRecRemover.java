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
        Map<String, List<Long>> deletedRecords_Global = new HashMap<>();

        // Первая таблица в задание - та, которую передали в tableName
        List<Long> recordsDelete = new ArrayList<>();
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
            Map<String, List<Long>> deletedRecords = new HashMap<>();
            deletedRecords.put(tableName, recordsDelete);

            // Переходим от текущего слоя ссылок переходим к следующему, пока на очередном слое не останется сылок
            do {
                Map<String, List<Long>> deletedRecords_Curr = new HashMap<>();

                // Собираем зависимости от таблиц из текуших deletedRecords
                Collection<String> refTables = deletedRecords.keySet();
                for (String tableNameRef : refTables) {
                    List<Long> recordsDeleteRef = deletedRecords.get(tableNameRef);

                    // Если есть записи по ссылке, то разматываем её
                    if (recordsDeleteRef.size() > 0) {
                        //
                        Map<String, List<Long>> deletedRecords_Ref = utRecMerger.loadRecordsRefTable(tableNameRef, recordsDeleteRef, dataSerializer, recMergeResultWriter, MergeOprType.DEL);

                        //
                        log.debug(levelIndent + "Dependences for: " + tableNameRef + ", count: " + recordsDeleteRef.size() + ", ids: " + recordsDeleteRef);
                        if (deletedRecords_Ref.keySet().size() == 0) {
                            log.debug(levelIndent + "  " + "<no ref>");
                        } else {
                            for (String tableNameRef_refTableName : deletedRecords_Ref.keySet()) {
                                List<Long> deletedRecordsInTable_RefRef = deletedRecords_Ref.get(tableNameRef_refTableName);
                                log.debug(levelIndent + "  " + tableNameRef_refTableName + ", count: " + deletedRecordsInTable_RefRef.size() + ", ids: " + deletedRecordsInTable_RefRef);
                            }
                        }

                        //
                        UtRecMerger.mapMergeMap(deletedRecords_Ref, deletedRecords_Curr);
                    }
                }

                // Уберем из deletedRecords_Curr значения, которые
                // 1) уже есть в deletedRecords_Global
                // 1) указаны в recordsDelete (записи из основной таблицы)
                clearAlreadyExists(deletedRecords_Curr.get(tableName), recordsDelete);
                clearAlreadyExists(deletedRecords_Curr, deletedRecords_Global);

                // Пополняем общий список зависимостей
                UtRecMerger.mapMergeMap(deletedRecords_Curr, deletedRecords_Global);

                // Теперь переходим на зависимости следующего слоя
                deletedRecords = deletedRecords_Curr;
                //
                levelIndent = levelIndent + "  ";
            } while (deletedRecords.size() != 0);

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
            Set<String> deletedTables_Global = deletedRecords_Global.keySet();
            List<String> refsToTableSorted = UtJdx.getSortedKeys(struct.getTables(), deletedTables_Global);
            for (int i = refsToTableSorted.size() - 1; i >= 0; i--) {
                String tableNameRef = refsToTableSorted.get(i);
                List<Long> deletedRecords = deletedRecords_Global.get(tableNameRef);
                if (deletedRecords.size() != 0) {
                    log.info("delete from: " + tableNameRef + ", count: " + deletedRecords.size() + ", ids: " + deletedRecords);
                    // Удаляем в обратном порядке, т.к. в конец были добавлены id "конечных" записей.
                    // Актуально для иерархических таблиц: при наличии цепочки дочерних записей их нужно удалять с конца.
                    Collections.reverse(deletedRecords);
                    // Удаляем
                    utRecMerger.execRecordsDelete(tableNameRef, deletedRecords);
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

    private void clearAlreadyExists(List<Long> idsNew, List<Long> listFull) {
        // Проиндексируем
        Set<Long> setFull = new HashSet<>(listFull);

        // Проверим
        if (idsNew != null) {
            for (int i = idsNew.size() - 1; i >= 0; i--) {
                if (setFull.contains(idsNew.get(i))) {
                    idsNew.remove(i);
                }
            }
        }
    }

    private void clearAlreadyExists(Map<String, List<Long>> list, Map<String, List<Long>> listFull) {
        for (String tableName : listFull.keySet()) {
            clearAlreadyExists(list.get(tableName), listFull.get(tableName));
        }
    }

}
