package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
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


    public void relocateIdList(String tableName, List<String> idsSour, List<String> idsDest, String resultDirName) throws Exception {
        if (idsDest.size() != idsSour.size()) {
            throw new XError("Не совпадает количество [idsSour] и [idsDest]");
        }

        //
        for (int i = 0; i < idsSour.size(); i++) {
            long idSour = Long.parseLong(idsSour.get(i));
            long idDest = Long.parseLong(idsDest.get(i));

            //
            System.out.println("  " + tableName + "." + idSour + " -> " + idDest);

            //
            try {
                File resultFile = new File(resultDirName + "relocate_" + tableName + "_" + idSour + "_" + idDest + ".zip");
                // Не затирать существующий
                if (resultFile.exists()) {
                    throw new XError("Файл уже существует: " + resultFile.getCanonicalPath());
                }

                //
                relocateId(tableName, idSour, idDest, resultFile);
            } catch (Exception e) {
                File resultFile = new File(resultDirName + "relocate_" + tableName + "_" + idSour + "_" + idDest + ".error");
                String errorText = UtJdxErrors.collectExceptionText(e);
                UtFile.saveString(errorText, resultFile);
                //
                System.out.println(errorText);

            }
        }
    }

    public void rec_relocate_paramsFile(String fileName, List<String> idsSour, List<String> idsDest) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] sArr = line.split(";|,|\\t| ");
                if (sArr.length >= 2) {
                    idsSour.add(sArr[0]);
                    idsDest.add(sArr[1]);
                }
            }
        } finally {
            bufferedReader.close();
        }
    }

    public void rec_relocate_paramsStr(String idSourStr, String idDestStr, List<String> idsSour, List<String> idsDest) throws Exception {
        String[] idDestArr = idDestStr.split(",");
        String[] idSourArr = idSourStr.split(",");
        //
        if (idDestArr.length != idSourArr.length) {
            throw new XError("Не совпадает количество [sour] и [dest]");
        }
        //
        idsSour.addAll(Arrays.asList(idSourArr));
        idsDest.addAll(Arrays.asList(idDestArr));
    }


    public void rec_relocate_paramsRange(String tableName, long idSourFrom, long idSourTo, long idDestFrom, List<String> idsSour, List<String> idsDest) throws Exception {
        // Селектим записи, подлежащие переносу (rangeFrom .. rangeTo)
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        String sqlSelectSour = "select " + pkFieldName + " from " + tableName + " where " + pkFieldName + " >= " + idSourFrom + " and " + pkFieldName + " <= " + idSourTo;
        DataStore stSour = db.loadSql(sqlSelectSour);

        // Проверим, что есть место, куда переносить
        long idDestTo = idDestFrom + stSour.size() - 1;
        String sqlSelectDest = "select " + pkFieldName + " from " + tableName + " where " + pkFieldName + " >= " + idDestFrom + " and " + pkFieldName + " <= " + idDestTo;
        DataStore stDest = db.loadSql(sqlSelectDest);
        if (stDest.size() != 0) {
            throw new XError("В диапазоне [" + idDestFrom + ".." + idDestTo + "], передлагаемом для размещения, уже есть записи");
        }

        // Собираем id
        collectPkVakues(stSour, pkFieldName, idsSour);

        // Формируем idsDest, начиная с idDestFrom
        for (int i = 0; i < idsSour.size(); i++) {
            idsDest.add(String.valueOf(idDestFrom));
            idDestFrom = idDestFrom + 1;
        }
    }


    /**
     * Перемещатель primary key записей.
     * Метод relocateId принимет параметром одно число, а методы, которые он использует - умеет и с несколькими.
     * Переносим по одной записи, потому что методы, которые использует relocateId предполагают именно MERGE,
     * т.е.объединение нескольких записей в одну эталонную.
     * Перенос id это просто частный случай MERGE.
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
            // 
            IJdxTable table = struct.getTable(tableName);
            if (table == null) {
                throw new XError("Таблица [" + tableName + "] - не найдена в базе данных");
            }
            String pkFieldName = table.getPrimaryKey().get(0).getName();
            String sqlSelectRec = "select * from " + tableName + " where " + pkFieldName + " = :" + pkFieldName;
            // Проверяем, что idSour не пустая
            DataStore stSour = db.loadSql(sqlSelectRec, UtCnv.toMap(pkFieldName, idSour));
            if (stSour.size() == 0) {
                throw new XError("Error relocateId: sour id not found: " + tableName + "." + idSour );
            }
            // Проверяем, что idDest свободна
            DataStore stDest = db.loadSql(sqlSelectRec, UtCnv.toMap(pkFieldName, idDest));
            if (stDest.size() != 0) {
                throw new XError("Error relocateId: dest id already exists: " + tableName + "." + idDest );
            }


            // Копируем запись tableName.idSour в tableName.idDest
            DataRecord recSour = stSour.get(0);
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

    private void collectPkVakues(DataStore store, String pkFieldName, Collection<String> res) {
        for (DataRecord rec : store) {
            if (rec.isValueNull(pkFieldName)) {
                continue;
            }
            String valueStr = rec.getValueString(pkFieldName);
            if (!res.contains(valueStr)) {
                res.add(valueStr);
            }
        }
    }

}
