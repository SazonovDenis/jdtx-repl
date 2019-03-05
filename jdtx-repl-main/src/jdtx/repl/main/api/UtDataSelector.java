package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

public class UtDataSelector {

    Db db;
    IJdxDbStruct struct;

    public UtDataSelector(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }

    /**
     * Обязана обеспечить правильный поток записей, если есть ссылка на самого себя
     */
    public void readFullData(String tableName, String tableFields, JdxReplicaWriterXml dataContainer) throws Exception {
        //
        DbQuery rsTableLog = selectFullData(tableName, tableFields);
        try {
            dataContainer.startTable(tableName);

            // измененные данные помещаем в dataContainer
            while (!rsTableLog.eof()) {
                dataContainer.append();
                // Тип операции
                dataContainer.setOprType(JdxOprType.OPR_INS);
                // Тело записи
                String[] tableFromFields = tableFields.split(",");
                for (String field : tableFromFields) {
                    dataContainer.setRecValue(field, rsTableLog.getValue(field));
                }
                //
                rsTableLog.next();
            }

            //
            dataContainer.flush();
        } finally {
            rsTableLog.close();
        }
    }

    protected DbQuery selectFullData(String tableName, String tableFields) throws Exception {
        //
        IJdxTableStruct tableFrom = struct.getTable(tableName);

        //
        String query = getSql(tableFrom, tableFields);

        //
        return db.openSql(query);
    }

    protected String getSql(IJdxTableStruct tableFrom, String tableFields) {
        // Пока так реализуем правильный поток записей, если есть ссылка на самого себя
        return "select " + tableFields + " from " + tableFrom.getName() + " order by id";
    }


}
