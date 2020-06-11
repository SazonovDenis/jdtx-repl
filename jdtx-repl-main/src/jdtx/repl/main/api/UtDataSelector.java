package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

import java.util.*;

public class UtDataSelector {

    private Db db;
    private IJdxDbStruct struct;
    private long wsId;

    private long MAX_SNAPSHOT_RECS = 5000;

    //
    protected static Log log = LogFactory.getLog("jdtx.DataSelector");


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
        // DbQuery, содержащие все данные из таблицы tableName
        IJdxDataBinder rsTableLog = selectAllRecords(tableName, tableFields);

        //
        try {
            flushDataToWriter(rsTableLog, tableName, tableFields, dataWriter, true);
        } finally {
            rsTableLog.close();
        }
    }

    public void readRecordsByIdList(String tableName, Collection<Long> idList, String tableFields, JdxReplicaWriterXml dataWriter) throws Exception {
        // Итератор, содержащий данные по списку
        IJdxDataBinder rsTableLog = new JdxDataBinder_SelectorById(db, tableName, tableFields, idList);

        //
        try {
            flushDataToWriter(rsTableLog, tableName, tableFields, dataWriter, false);
        } finally {
            rsTableLog.close();
        }
    }


    private void flushDataToWriter(IJdxDataBinder data, String tableName, String tableFields, JdxReplicaWriterXml dataWriter, boolean forbidNotOwnId) throws Exception {
        IJdxTable table = struct.getTable(tableName);
        IRefDecoder decoder = new RefDecoder(db, wsId);
        UtDataWriter utDataWriter = new UtDataWriter(table, tableFields, decoder, forbidNotOwnId);

        //
        dataWriter.startTable(tableName);

        // Данные помещаем в dataWriter
        long count = 0;
        long countPortion = 0;
        while (!data.eof()) {
            // Обеспечим не слишком огромные порции данных
            if (countPortion >= MAX_SNAPSHOT_RECS) {
                countPortion = 0;
                dataWriter.startTable(tableName);
            }
            countPortion++;

            // Добавляем запись
            dataWriter.appendRec();

            // Тип операции
            dataWriter.setOprType(JdxOprType.OPR_INS);

            // Тело записи
            utDataWriter.dataBinderRec_To_DataWriter_WithRefDecode(data, dataWriter);

            //
            data.next();

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
    }

    protected IJdxDataBinder selectAllRecords(String tableName, String tableFields) throws Exception {
        //
        IJdxTable tableFrom = struct.getTable(tableName);

        //
        String sql = getSql(tableFrom, tableFields);

        //
        DbQuery query = db.openSql(sql);

        //
        return new JdxDataBinder_DbQuery(query);
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

        // Порядок следования записей важен даже при получении snapshot,
        // т.к. важно обеспечить аправильный порядок вставки, например: триггер учитывает данные новой и ПРЕДЫДУЩЕЙ записи (см. например calc_SubjectOpr)
        return "select " + tableFields + " from " + tableFrom.getName() + " order by " + tableFrom.getPrimaryKey().get(0).getName();
    }


}
