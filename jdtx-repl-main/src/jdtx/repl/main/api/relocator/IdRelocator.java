package jdtx.repl.main.api.relocator;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.DbUtils;
import jdtx.repl.main.api.rec_merge.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 * Перемещатель id записей
 */
public class IdRelocator {

    Db db;
    DbUtils dbu;
    IJdxDbStruct struct;

    public IdRelocator(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
        dbu = new DbUtils(db, struct);
    }

    RecordsUpdatedMap relocateIdCheck(String tableName, long idSour, long idDest) throws Exception {
        RecordsUpdatedMap recordsUpdatedMap = new RecordsUpdatedMap();

        db.startTran();
        try {
            String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();

            // Проверяем, что idSour не пустая
            String sql = "select * from " + tableName + " where " + pkField + " = :" + pkField;
            DataRecord rec = dbu.loadSqlRec(sql, UtCnv.toMap(pkField, idSour));

            // Проверякм все ссылки tableName.idSour на tableName.idDest
            for (IJdxTable refTable : struct.getTables()) {

                for (IJdxForeignKey ref : refTable.getForeignKeys()) {
                    if (ref.getTable().getName().compareToIgnoreCase(tableName) == 0 && ref.getTableField().getName().compareToIgnoreCase(pkField) == 0) {
                        String refFieldName = ref.getField().getName();
                        String refTableName = refTable.getName();
                        //
                        String sqlSelect = "select * from " + refTableName + " where " + refFieldName + " = :" + refFieldName + "_sour";
                        Map paramsSelect = UtCnv.toMap(refFieldName + "_sour", idSour);
                        DataStore refData = db.loadSql(sqlSelect, paramsSelect);
                        //
                        RecordsUpdated recordsUpdated = recordsUpdatedMap.addForTable(refTableName, refFieldName);
                        recordsUpdated.recordsUpdated = refData;
                    }
                }
            }

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }


        //
        return recordsUpdatedMap;
    }

    void relocateId(String tableName, long idSour, long idDest) throws Exception {
        if (idSour == idDest) {
            throw new XError("Error relocateId: idSour == idDest");
        }
        if (idSour == 0) {
            throw new XError("Error relocateId: idSour == 0");
        }
        if (idDest == 0) {
            throw new XError("Error relocateId: idDest == 0");
        }


        db.startTran();
        try {
            String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();

            // Проверяем, что idSour не пустая
            String sql = "select * from " + tableName + " where " + pkField + " = :" + pkField;
            DataRecord rec = dbu.loadSqlRec(sql, UtCnv.toMap(pkField, idSour));

            // Копируем запись tableName.idSour в tableName.idDest
            rec.setValue(pkField, idDest);
            dbu.insertRec(tableName, rec.getValues());

            // Перебиваем все ссылки tableName.idSour на tableName.idDest
            for (IJdxTable refTable : struct.getTables()) {

                for (IJdxForeignKey ref : refTable.getForeignKeys()) {
                    if (ref.getTable().getName().compareToIgnoreCase(tableName) == 0 && ref.getTableField().getName().compareToIgnoreCase(pkField) == 0) {
                        String refTableName = refTable.getName();
                        String refFieldName = ref.getField().getName();
                        //
                        String sqlUpdate = "update " + refTableName + " set " + refFieldName + " = :" + refFieldName + "_dest" + " where " + refFieldName + " = :" + refFieldName + "_sour";
                        Map params = UtCnv.toMap(refFieldName + "_sour", idSour, refFieldName + "_dest", idDest);
                        db.execSql(sqlUpdate, params);
                    }
                }
            }

            // Удаляем старую запись tableName.idSour
            dbu.deleteRec(tableName, idSour);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }

}
