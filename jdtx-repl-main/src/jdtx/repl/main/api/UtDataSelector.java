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

    public void readFullData(String tableName, String tableFields, JdxDataWriter dataContainer) throws Exception {
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
        return "select " + tableFields + " from " + tableFrom.getName();
    }


}
