package jdtx.repl.main.api;

import jandcode.dbm.db.Db;
import jandcode.dbm.db.DbQuery;
import jdtx.repl.main.api.struct.IJdxDbStruct;
import jdtx.repl.main.api.struct.IJdxTableStruct;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
     * Обязана обеспечить правильный поток записей, если есть ссылка на самого себя
     */
    public void readAllRecords(String tableName, String tableFields, JdxReplicaWriterXml dataContainer) throws Exception {
        DbQuery rsTableLog = selectAllRecords(tableName, tableFields);
        try {
            dataContainer.startTable(tableName);

            // измененные данные помещаем в dataContainer
            long n = 0;
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

                //
                n++;
                if (n % 1000 == 0) {
                    log.info("readData: " + tableName + ", " + n);
                }
            }

            //
            log.info("readData: " + tableName + ", total: " + n);

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
        // Пока так реализуем правильный поток записей, если есть ссылка на самого себя
        return "select " + tableFields + " from " + tableFrom.getName() + " order by id";
    }


}
