package jdtx.repl.main.api.relocator;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
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
        dbu = new DbUtils(db);
    }

    MergeResultTable relocateIdCheck(String tableName, long idSour, long idDest) throws Exception {
        return null;
    }

    void relocateId(String tableName, long idSour, long idDest) throws Exception {
        db.startTran();
        try {
            String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();

            // Проверяем, что idSour не пустая
            DataRecord rec = dbu.loadSqlRec(tableName, UtCnv.toMap(pkField, idSour));

            // Копируем запись tableName.idSour в tableName.idDest
            rec.setValue(pkField, idDest);
            dbu.insertRec(tableName, rec);

            // Перебиваем все ссылки tableName.idSour на tableName.idDest
            for (IJdxTable refTable : struct.getTables()) {

                for (IJdxForeignKey ref : refTable.getForeignKeys()) {
                    if (ref.getTable().getName().compareToIgnoreCase(tableName) == 0 && ref.getTableField().getName().compareToIgnoreCase(pkField) == 0) {
                        String sqlSelect = "select * from " + refTable.getName() + " where " + ref.getField().getName() + " = :" + ref.getField().getName() + "_sour";
                        Map paramsSelect = UtCnv.toMap(ref.getField().getName() + "_sour", idSour);
                        DataStore refData = dbu.loadSql(sqlSelect, paramsSelect);
                        dbu.outTable(refData);

                        String sqlUpdate = "update " + refTable.getName() + " set " + ref.getField().getName() + " = :" + ref.getField().getName() + "_dest" + " where " + ref.getField().getName() + " = :" + ref.getField().getName() + "_sour";
                        Map params = UtCnv.toMap(ref.getField().getName(), idSour);
                        dbu.execSql(sqlUpdate, params);
                    }
                }
            }

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }

}
