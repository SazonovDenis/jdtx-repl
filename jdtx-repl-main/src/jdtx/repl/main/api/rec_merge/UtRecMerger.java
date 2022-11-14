package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.util.*;


public class UtRecMerger {

    Db db;
    JdxDbUtils dbu;
    IJdxDbStruct struct;

    //
    protected static Log log = LogFactory.getLog("jdtx.UtRecMerger");

    //
    public UtRecMerger(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
        this.dbu = new JdxDbUtils(db, struct);
    }

    /**
     * Сохраняем все записи в зависимых таблицах, которые ссылаются на записи records из tableName.
     * Возвращает Map <имя зависимой таблицы, набор её обновляемых/удаленных записей>.
     *
     * @param tableName    имя таблицы
     * @param records      обновляемые или удаляемые записи в этой таблице
     * @param resultWriter место для сохранения исходного состояния обновляемых/удаленных записей
     * @return Набор id для каждой зависомой таблицы
     */
    public Map<String, List<Long>> loadRecordsRefTable(String tableName, Collection<Long> records, IJdxDataSerializer dataSerializer, RecMergeResultWriter resultWriter, MergeOprType writerMode) throws Exception {
        Map<String, List<Long>> deletedRecordsInTables = new HashMap<>();

        // Собираем непосредственные зависимости
        Map<String, Collection<IJdxForeignKey>> refsToTable = UtJdx.getRefsToTable(struct.getTables(), struct.getTable(tableName), false);
        Collection<String> refsToTableKeys = refsToTable.keySet();
        // Строго по порядку fk
        List<String> refsToTableSorted = UtJdx.getSortedKeys(struct.getTables(), refsToTableKeys);

        // Обрабатываем зависимости
        for (String refTableName : refsToTableSorted) {
            IJdxTable refTable = struct.getTable(refTableName);
            String refTablePkFieldName = refTable.getPrimaryKey().get(0).getName();

            // Таблица и поля в Serializer-е
            if (resultWriter != null) {
                dataSerializer.setTable(refTable, UtJdx.fieldsToString(refTable.getFields()));
            }

            //
            Collection<IJdxForeignKey> refsToTableFkList = refsToTable.get(refTableName);
            for (IJdxForeignKey fk : refsToTableFkList) {
                String refFkFieldName = fk.getField().getName();

                // Тут собираем id удаленных записей в таблице refTableName только по ссылке refFkFieldName
                List<Long> deletedRecordsInTable_forRef = new ArrayList<>();

                // Таблица во Writer-е
                if (resultWriter != null) {
                    String refInfo = "ref: " + refTableName + "." + refFkFieldName + "--" + tableName;
                    resultWriter.writeTableItem(new MergeResultTableItem(refTableName, writerMode, refInfo));
                }

                //
                log.debug("Dependence: " + refTableName + "." + refFkFieldName + " --> " + tableName);

                // Селектим из refTableName по ссылке refFkFieldName, записываем в resultWriter и deletedRecordsInTable_forRef
                String sqlSelect = "select * from " + refTableName + " where " + refFkFieldName + " = :" + refFkFieldName;
                long countTotal = 0;
                for (long recordId : records) {
                    // Селектим как есть сейчас
                    Map params = UtCnv.toMap(refFkFieldName, recordId);
                    DbQuery query = db.openSql(sqlSelect, params);

                    try {
                        // Записываем
                        long count = 0;
                        while (!query.eof()) {
                            Map<String, Object> values = query.getValues();
                            // Сохраняем всю запись в resultWriter
                            if (resultWriter != null) {
                                Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
                                resultWriter.writeRec(valuesStr);
                            }
                            // Собираем только id записи
                            long id = UtJdxData.longValueOf(values.get(refTablePkFieldName));
                            deletedRecordsInTable_forRef.add(id);
                            //
                            count = count + 1;
                            //
                            query.next();
                        }

                        //
                        log.debug("  ref: " + recordId + ", records: " + count);
                    } finally {
                        query.close();
                    }

                    //
                    countTotal++;
                    if (countTotal % 1000 == 0) {
                        log.info("  " + countTotal + " / " + records.size() + ", found: " + deletedRecordsInTable_forRef.size());
                    }
                }

                // Тут собираем id удаленных записей в таблице refTableName не только по ссылке refFkFieldName,
                // а по всем ссылкам из таблицы refTableName
                UtRecMerger.mapMergeList(refTableName, deletedRecordsInTable_forRef, deletedRecordsInTables);
            }
        }

        //
        return deletedRecordsInTables;
    }

    /**
     * К мапе mapDest, содержащей списки,
     * добавляет список sourceValue по ключу sourceKey таким образом,
     * что уже имеющееся значения по ключу sourceKey не затираются, а пополняется
     *
     * @param sourceKey   ключ для добалнеия в mapDest
     * @param sourceValue добавляемый список
     * @param mapDest     пополняемая Map со списками
     */
    public static void mapMergeList(String sourceKey, List<Long> sourceValue, Map<String, List<Long>> mapDest) {
        List<Long> destValue = mapDest.get(sourceKey);
        if (destValue == null) {
            destValue = new ArrayList<>();
            mapDest.put(sourceKey, destValue);
        }

        destValue.addAll(sourceValue);
    }

    /**
     * Сливает mapSource (содержащие списки) таким образом,
     * что уже имеющиеся значения в списках не затираются, а объединяются.
     *
     * @param mapSource добавляемые списки
     * @param mapDest   пополняемая Map со списками
     */
    public static void mapMergeMap(Map<String, List<Long>> mapSource, Map<String, List<Long>> mapDest) {
        for (String key : mapSource.keySet()) {
            List<Long> destList = mapDest.get(key);
            List<Long> addList = mapSource.get(key);
            if (destList == null) {
                mapDest.put(key, addList);
            } else {
                destList.addAll(addList);
            }
        }
    }


    /**
     * Сохраняем записи records из tableName
     */
    public void saveRecordsTable(String tableName, Collection<Long> records, IJdxDataSerializer dataSerializer, RecMergeResultWriter resultWriter) throws Exception {
        // Таблица и поля в Serializer-е
        IJdxTable table = struct.getTable(tableName);
        dataSerializer.setTable(table, UtJdx.fieldsToString(table.getFields()));
        String pkFieldName = struct.getTable(tableName).getPrimaryKey().get(0).getName();

        // Таблица во Writer-е
        resultWriter.writeTableItem(new MergeResultTableItem(tableName, MergeOprType.DEL));

        // Селектим из tableName и записываем в resultWriter
        String sqlSelect = "select * from " + tableName + " where " + pkFieldName + " = :" + pkFieldName;
        for (long deleteRecId : records) {
            // Селектим как есть сейчас
            Map params = UtCnv.toMap(pkFieldName, deleteRecId);
            DataStore store = db.loadSql(sqlSelect, params);

            // Сохраняем всю запись в resultWriter
            for (DataRecord rec : store) {
                Map<String, Object> values = rec.getValues();
                Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
                resultWriter.writeRec(valuesStr);
            }
        }
    }

    public void execRecordsUpdateRefs(String tableName, Collection<Long> recordsDelete, long etalonRecId) throws Exception {
        // Собираем непосредственные зависимости
        Map<String, Collection<IJdxForeignKey>> refsToTable = UtJdx.getRefsToTable(struct.getTables(), struct.getTable(tableName), false);

        // Обрабатываем зависимости
        for (String refTableName : refsToTable.keySet()) {
            Collection<IJdxForeignKey> fkList = refsToTable.get(refTableName);
            for (IJdxForeignKey fk : fkList) {
                String refFieldName = fk.getField().getName();
                String sqlUpdate = "update " + refTableName + " set " + refFieldName + " = :" + refFieldName + "_NEW" + " where " + refFieldName + " = :" + refFieldName + "_OLD";

                //
                for (long deleteRecId : recordsDelete) {
                    Map params = UtCnv.toMap(
                            refFieldName + "_OLD", deleteRecId,
                            refFieldName + "_NEW", etalonRecId
                    );

                    // Апдейтим
                    db.execSql(sqlUpdate, params);
                }
            }
        }
    }

    /**
     * Удаляем записи recordsDelete из tableName
     */
    public void execRecordsDelete(String tableName, List<Long> recordsDelete) throws Exception {
        String pkFieldName = struct.getTable(tableName).getPrimaryKey().get(0).getName();
        String sqlDelete = "delete from " + tableName + " where " + pkFieldName + " = :" + pkFieldName;

        //
        for (long deleteRecId : recordsDelete) {
            Map params = UtCnv.toMap(pkFieldName, deleteRecId);

            // Удаляем
            db.execSql(sqlDelete, params);
        }
    }

}
