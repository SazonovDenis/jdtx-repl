package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;

import java.util.*;


public class UtRecMerger {

    Db db;
    JdxDbUtils dbu;
    IJdxDbStruct struct;

    public UtRecMerger(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
        this.dbu = new JdxDbUtils(db, struct);
    }

    /**
     * Сохраняем все записи в зависимых таблицах, которые ссылаются на записи records из tableName.
     * Возвращает Map <имя зависимой таблицы, набор её обновляемых/удаленных записей>.
     *
     * @param tableName    имя таблицы
     * @param records      обновляемые или удаляемые записи
     * @param resultWriter место для сохранения исходного состояния обновляемых/удаленных записей
     * @return Набор id для каждой зависомой таблицы
     */
    public Map<String, Set<Long>> saveRecordsRefTable(String tableName, Collection<Long> records, RecMergeResultWriter resultWriter, MergeOprType writerMode, IJdxDataSerializer dataSerializer) throws Exception {
        Map<String, Set<Long>> deletedRecordsInTables = new HashMap<>();

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
            dataSerializer.setTable(refTable, UtJdx.fieldsToString(refTable.getFields()));

            //
            Collection<IJdxForeignKey> refsToTableFkList = refsToTable.get(refTableName);
            for (IJdxForeignKey fk : refsToTableFkList) {
                String refFkFieldName = fk.getField().getName();

                // Тут собираем только id удаленных записей в таблице refTableName
                Set<Long> deletedRecordsInTable = new HashSet<>();
                deletedRecordsInTables.put(refTableName, deletedRecordsInTable);

                // Таблица во Writer-е
                String refInfo = "ref: " + refTableName + "." + refFkFieldName + "--" + tableName;
                resultWriter.writeTableItem(new MergeResultTableItem(refTableName, writerMode, refInfo));

                // Селектим из refTableName по ссылке refFkFieldName, записываем в resultWriter и deletedRecordsInTable
                String sqlSelect = "select * from " + refTableName + " where " + refFkFieldName + " = :" + refFkFieldName;
                for (long recordId : records) {
                    // Селектим как есть сейчас
                    Map params = UtCnv.toMap(refFkFieldName, recordId);
                    DbQuery query = db.openSql(sqlSelect, params);

                    // Записываем
                    while (!query.eof()) {
                        // Сохраняем всю запись в resultWriter
                        Map<String, Object> values = query.getValues();
                        Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
                        resultWriter.writeRec(valuesStr);
                        // Собираем только id записи
                        long id = UtJdxData.longValueOf(valuesStr.get(refTablePkFieldName));
                        deletedRecordsInTable.add(id);
                        //
                        query.next();
                    }
                }
            }
        }

        //
        return deletedRecordsInTables;
    }

    /**
     * Сохраняем записи records из tableName
     */
    public void saveRecordsTable(String tableName, Collection<Long> records, RecMergeResultWriter resultWriter, IJdxDataSerializer dataSerializer) throws Exception {
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
    public void execRecordsDelete(String tableName, Collection<Long> recordsDelete) throws Exception {
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
