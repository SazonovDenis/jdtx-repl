package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public class UtDataSelector {

    private Db db;
    private IJdxDbStruct struct;
    private long wsId;

    //
    protected static Log log = LogFactory.getLog("jdtx");


    //
    public UtDataSelector(Db db, IJdxDbStruct struct, long wsId) {
        this.db = db;
        this.struct = struct;
        this.wsId = wsId;
    }

    /**
     * Если в таблице есть ссылка на самого себя, процедура обязана обеспечить правильную последовательность записей.
     */
    public void readAllRecords(String tableName, String tableFields, JdxReplicaWriterXml dataWriter) throws Exception {
        IJdxTable table = struct.getTable(tableName);

        //
        IRefDecoder decoder = new RefDecoder(db, wsId);

        // DbQuery, содержащие все данные
        DbQuery rsTableLog = selectAllRecords(tableName, tableFields);

        //
        try {
            dataWriter.startTable(tableName);

            // Данные помещаем в dataWriter
            long count = 0;
            while (!rsTableLog.eof()) {
                dataWriter.appendRec();

                // Тип операции
                dataWriter.setOprType(JdxOprType.OPR_INS);

                // Тело записи
                String[] tableFromFields = tableFields.split(",");
                for (String fieldName : tableFromFields) {
                    Object fieldValue = rsTableLog.getValue(fieldName);

                    // Защита от дурака: в snapshot недопустимы чужие id
                    IJdxField field = table.getField(fieldName);
                    if (field.isPrimaryKey()) {
                        String refTableName = table.getName();
                        long own_id = Long.valueOf(String.valueOf(fieldValue));
                        if (!decoder.is_own_id(refTableName, own_id)) {
                            throw new XError("Not own id found, tableName: " + refTableName + ", id: " + own_id);
                        }
                    }

                    //
                    dataWriter.setRecValue(fieldName, fieldValue);
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
            dataWriter.flush();
        } finally {
            rsTableLog.close();
        }
    }

    protected DbQuery selectAllRecords(String tableName, String tableFields) throws Exception {
        //
        IJdxTable tableFrom = struct.getTable(tableName);

        //
        String query = getSql(tableFrom, tableFields);

        //
        return db.openSql(query);
    }

    protected String getSql(IJdxTable tableFrom, String tableFields) {
        for (IJdxForeignKey fk : tableFrom.getForeignKeys()) {
            if (fk.getTable().getName().equals(tableFrom.getName())) {
                // todo: Пока так реализуем правильную последовательность записей (если есть ссылка на самого себя)
                return "select 0 as dummySortField, " + tableFields + " from " + tableFrom.getName() + " where " + fk.getField().getName() + " = 0\n" +
                        "union\n" +
                        "select 1 as dummySortField, " + tableFields + " from " + tableFrom.getName() + " where " + fk.getField().getName() + " <> 0\n" +
                        "order by 1";
            }
        }
        //
        return "select " + tableFields + " from " + tableFrom.getName();
    }


}
