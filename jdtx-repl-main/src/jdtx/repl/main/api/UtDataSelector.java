package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public class UtDataSelector {

    Db db;
    IJdxDbStruct struct;

    //
    protected static Log log = LogFactory.getLog("jdtx");


    //
    public UtDataSelector(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }

    /**
     * Если в таблице есть ссылка на самого себя, обязана обеспечить правильную последовательность записей.
     */
    public void readAllRecords(String tableName, String tableFields, JdxReplicaWriterXml dataContainer) throws Exception {
        DbQuery rsTableLog = selectAllRecords(tableName, tableFields);
        try {
            dataContainer.startTable(tableName);

            // измененные данные помещаем в dataContainer
            long count = 0;
            while (!rsTableLog.eof()) {
                dataContainer.appendRec();
                // Тип операции
                dataContainer.setOprType(JdxOprType.OPR_INS);
                // Тело записи
                String[] tableFromFields = tableFields.split(",");
                for (String field : tableFromFields) {
                    dataContainer.setRecValue(field, rsTableLog.getValue(field));
                }
                //
                rsTableLog.next();

                //
                count++;
                if (count % 1000 == 0) {
                    log.info("  readData: " + tableName + ", " + count);
                }
            }

            //
            log.info("  readData: " + tableName + ", total: " + count);

            //
            dataContainer.flush();
        } finally {
            rsTableLog.close();
        }
    }

    protected DbQuery selectAllRecords(String tableName, String tableFields) throws Exception {
        //
        IJdxTableStruct tableFrom = struct.getTable(tableName);

        //
        String query = getSql(tableFrom, tableFields);

        //
        return db.openSql(query);
    }

    protected String getSql(IJdxTableStruct tableFrom, String tableFields) {
        for (IJdxForeignKey fk : tableFrom.getForeignKeys()) {
            if (fk.getTable().getName().equals(tableFrom.getName())) {
                // todo: Пока так реализуем правильную последовательность записей (если есть ссылка на самого себя)
                return "select 0 as dummySortField, " + tableFields + " from " + tableFrom.getName() + " where " + fk.getField().getName() + " = 0\n" +
                        "union\n" +
                        "select 1 as dummySortField, " + tableFields + " from " + tableFrom.getName() + " where " + fk.getField().getName() + " <> 0\n"+
                        "order by 1";
            }
        }
        //
        return "select " + tableFields + " from " + tableFrom.getName();
    }


}
