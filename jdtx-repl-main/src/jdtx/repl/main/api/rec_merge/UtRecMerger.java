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
     * Вытаскиват все, что нужно будет обновить (в разных таблицах),
     * если делать relocate/delete записи idSour в таблице tableName
     */
    public void recordsRelocateSave(String tableName, Collection<Long> recordsDelete, IJdxDataSerializer dataSerializer, RecMergeResultWriter resultWriter) throws Exception {
        // Собираем непосредственные зависимости
        Map<String, Collection<IJdxForeignKey>> refsToTable = UtJdx.getRefsToTable(struct.getTables(), struct.getTable(tableName), false);

        // Обрабатываем зависимости
        for (String refTableName : refsToTable.keySet()) {
            Collection<IJdxForeignKey> fkList = refsToTable.get(refTableName);
            for (IJdxForeignKey fk : fkList) {
                String refFieldName = fk.getField().getName();
                String sqlSelect = "select * from " + refTableName + " where " + refFieldName + " = :" + refFieldName;

                //
                for (long deleteRecId : recordsDelete) {
                    // Селектим как есть сейчас
                    Map params = UtCnv.toMap(refFieldName, deleteRecId);
                    DbQuery stUpdated = db.openSql(sqlSelect, params);

                    // Отчитаемся
                    resultWriter.writeTableItem(new MergeResultTableItem(refTableName, MergeOprType.UPD));
                    while (!stUpdated.eof()) {
                        Map<String, Object> values = stUpdated.getValues();
                        Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
                        resultWriter.writeRec(valuesStr);
                        //
                        stUpdated.next();
                    }
                }
            }
        }
    }

    public void recordsRelocateExec(String tableName, Collection<Long> recordsDelete, long etalonRecId) throws Exception {
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
     * Сохраняем записи recordsDelete из tableName
     */
    public void recordsDeleteSave(String tableName, Collection<Long> recordsDelete, IJdxDataSerializer dataSerializer, RecMergeResultWriter resultWriter) throws Exception {
        String pkFieldName = struct.getTable(tableName).getPrimaryKey().get(0).getName();
        String sqlSelect = "select * from " + tableName + " where " + pkFieldName + " = :" + pkFieldName;

        //
        resultWriter.writeTableItem(new MergeResultTableItem(tableName, MergeOprType.DEL));

        //
        for (long deleteRecId : recordsDelete) {
            Map params = UtCnv.toMap(pkFieldName, deleteRecId);

            // Селектим как есть сейчас
            DataStore store = db.loadSql(sqlSelect, params);

            // Отчитаемся
            for (DataRecord rec : store) {
                Map<String, Object> values = rec.getValues();
                Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
                resultWriter.writeRec(valuesStr);
            }
        }
    }

    /**
     * Удаляем записи recordsDelete из tableName
     */
    public void recordsDeleteExec(String tableName, Collection<Long> recordsDelete) throws Exception {
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
