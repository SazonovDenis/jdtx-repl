package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.pk_generator.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

public class JdxRecRelocator {


    Db db;
    JdxDbUtils dbu;
    IJdxDbStruct struct;
    IJdxDataSerializer dataSerializer;

    //
    protected static Log log = LogFactory.getLog("jdtx.UtRecRelocator");

    //
    public JdxRecRelocator(Db db, IJdxDbStruct struct, IJdxDataSerializer dataSerializer) {
        this.db = db;
        this.struct = struct;
        this.dbu = new JdxDbUtils(db, struct);
        this.dataSerializer = dataSerializer;
    }


    /**
     * Перемещатель id записей
     */
    public void relocateId(String tableName, long idSour, long idDest, File resultFile) throws Exception {
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
            // Проверяем, что idSour не пустая
            IJdxTable table = struct.getTable(tableName);
            String pkFieldName = table.getPrimaryKey().get(0).getName();
            String sql = "select * from " + tableName + " where " + pkFieldName + " = :" + pkFieldName;
            DataStore stSour = db.loadSql(sql, UtCnv.toMap(pkFieldName, idSour));
            if (stSour.size() == 0) {
                throw new XError("Error relocateId: idSour not found");
            }
            DataRecord recSour = stSour.get(0);


            // Копируем запись tableName.idSour в tableName.idDest
            recSour.setValue(pkFieldName, idDest);
            dbu.insertRec(tableName, recSour.getValues());

            //
            UtRecMerger utRecMerger = new UtRecMerger(db, struct);
            ArrayList<Long> recordsDelete = new ArrayList<>();
            recordsDelete.add(idSour);


            // Сохраняем результат выполнения задачи
            RecMergeResultWriter recMergeResultWriter = new RecMergeResultWriter();
            recMergeResultWriter.open(resultFile);

            // DEL - Сохраняем то, что нужно удалить
            utRecMerger.saveRecordsTable(tableName, recordsDelete, recMergeResultWriter, dataSerializer);
            // UPD - Сохраняем то, где нужно перебить ссылки
            utRecMerger.saveRecordsRefTable(tableName, recordsDelete, recMergeResultWriter, MergeOprType.UPD, dataSerializer);

            // Сохраняем
            recMergeResultWriter.close();


            // Перебиваем ссылки у зависимых таблиц с tableName.idSour на tableName.idDest
            utRecMerger.execRecordsUpdateRefs(tableName, recordsDelete, idDest);

            // Удаляем старую запись tableName.idSour
            utRecMerger.execRecordsDelete(tableName, recordsDelete);


            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }


    /**
     * Проверяет объем изменений, необходимых для перемещения записи idSour в таблице tableName
     */
    public void relocateIdCheck(String tableName, long idSour, File outFile) throws Exception {
        UtRecMerger utRecMerger = new UtRecMerger(db, struct);
        ArrayList<Long> recordsDelete = new ArrayList<>();
        recordsDelete.add(idSour);

        // Сохраняем результат выполнения задачи
        RecMergeResultWriter recMergeResultWriter = new RecMergeResultWriter();
        recMergeResultWriter.open(outFile);

        // DEL - Сохраняем то, что нужно удалить
        utRecMerger.saveRecordsTable(tableName, recordsDelete, recMergeResultWriter, dataSerializer);
        // UPD - Сохраняем то, где нужно перебить ссылки
        utRecMerger.saveRecordsRefTable(tableName, recordsDelete, recMergeResultWriter, MergeOprType.UPD, dataSerializer);

        // Сохраняем
        recMergeResultWriter.close();
    }

    public void relocateIdAll(String tableName, long maxPkValue, String resultDirName) throws Exception {
        System.out.println("References for table: " + tableName);

        // Селектим записи, подлежащие переносу (больше maxPkValue)
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        String sqlSelect = "select " + pkFieldName + " from " + tableName + " where " + pkFieldName + " >= " + maxPkValue;
        DataStore st = db.loadSql(sqlSelect);
        //
        Collection<Long> ids = new ArrayList<>();
        collectPkVakues(st, pkFieldName, ids);

        //
        System.out.println(tableName + ", count: " + ids.size());

        // Переносим
        PkGeneratorService svc = db.getApp().service(PkGeneratorService.class);
        IPkGenerator generator = svc.createGenerator(db, struct);
        long idDest = generator.getValue(generator.getGeneratorName(tableName));
        for (Long idSour : ids) {
            idDest = idDest + 1;
            generator.setValue(generator.getGeneratorName(tableName), idDest);
            //
            System.out.println("  " + tableName + "." + idSour + " -> " + idDest);
            //
            File resultFile = new File(resultDirName + "relocate_" + tableName + "_" + idSour + "_" + idDest + ".zip");
            relocateId(tableName, idSour, idDest, resultFile);
        }

    }

    private void collectPkVakues(DataStore store, String pkFieldName, Collection<Long> res) {
        for (DataRecord rec : store) {
            if (rec.isValueNull(pkFieldName)) {
                continue;
            }
            long value = rec.getValueLong(pkFieldName);
            if (!res.contains(value)) {
                res.add(value);
            }
        }
    }

}
