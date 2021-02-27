package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

import java.util.*;

public class UtDataSelector {

    private Db db;
    private IJdxDbStruct struct;
    private long wsId;
    private boolean forbidNotOwnId;
    private IRefDecoder decoder;

    private long MAX_SNAPSHOT_RECS = 5000;

    //
    protected static Log log = LogFactory.getLog("jdtx.DataSelector");


    //
    public UtDataSelector(Db db, IJdxDbStruct struct, long wsId, boolean forbidNotOwnId) throws Exception {
        this.db = db;
        this.struct = struct;
        this.wsId = wsId;
        this.forbidNotOwnId = forbidNotOwnId;
        this.decoder = new RefDecoder(db, wsId);
    }

    /**
     * Если в таблице есть ссылка на самого себя, процедура обязана обеспечить правильную последовательность записей.
     */
    public void readAllRecords(IPublicationRule publicationRule, JdxReplicaWriterXml dataWriter) throws Exception {
        // DbQuery, содержащие все данные из таблицы tableName
        IJdxDataBinder rsTableLog = selectAllRecords(publicationRule);

        //
        try {
            String tableName = publicationRule.getTableName();
            String tableFields = JdxUtils.fieldsToString(publicationRule.getFields());
            flushDataToWriter(rsTableLog, tableName, tableFields, dataWriter);
        } finally {
            rsTableLog.close();
        }
    }

    public void readRecordsByIdList(String tableName, Collection<Long> idList, String tableFields, JdxReplicaWriterXml dataWriter) throws Exception {
        // Итератор, содержащий данные по списку
        IJdxDataBinder rsTableLog = new JdxDataBinder_SelectorById(db, tableName, tableFields, idList);

        //
        try {
            flushDataToWriter(rsTableLog, tableName, tableFields, dataWriter);
        } finally {
            rsTableLog.close();
        }
    }


    private void flushDataToWriter(IJdxDataBinder data, String tableName, String tableFields, JdxReplicaWriterXml dataWriter) throws Exception {
        IJdxTable table = struct.getTable(tableName);
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

    protected IJdxDataBinder selectAllRecords(IPublicationRule publicationRule) throws Exception {
        //
        String sql = getSqlAllRecords(publicationRule);

        //
        DbQuery query = db.openSql(sql);

        //
        return new JdxDataBinder_DbQuery(query);
    }

    protected String getSqlAllRecords(IPublicationRule publicationRule) {
        String tableName = publicationRule.getTableName();
        IJdxTable tableFrom = struct.getTable(tableName);
        //
        String tableFields = JdxUtils.fieldsToString(publicationRule.getFields(), tableFrom.getName() + ".");
        //
        String condWhere = "";
        if (publicationRule.getAuthorWs() != null) {
            condWhere = "where z_z_decode.ws_id in (" + publicationRule.getAuthorWs() + ")\n";
        }

        // Таблица древовидная (имеет ссылки на саму себя)?
        for (IJdxForeignKey fk : tableFrom.getForeignKeys()) {
            if (fk.getTable().getName().equals(tableFrom.getName())) {
                // todo: Пока так реализуем правильную последовательность записей (если есть ссылка на самого себя)
                return "select\n" +
                        "  0 as dummySortField, \n" +
                        "  " + tableFields + "\n" +
                        "from\n" +
                        "  " + tableFrom.getName() + "\n" +
                        "  left join z_z_decode on (z_z_decode.table_name = '" + tableFrom.getName() + "' and z_z_decode.own_slot = (" + tableFrom.getName() + ".id / " + RefDecoder.SLOT_SIZE + "))\n" +
                        condWhere +
                        "where\n" +
                        "  " + fk.getField().getName() + " = 0\n" +
                        "\n" +
                        "union\n" +
                        "\n" +
                        "select\n" +
                        "  1 as dummySortField, " + tableFields + "\n" +
                        "from\n" +
                        "  " + tableFrom.getName() + "\n" +
                        "  left join z_z_decode on (z_z_decode.table_name = '" + tableFrom.getName() + "' and z_z_decode.own_slot = (" + tableFrom.getName() + ".id / " + RefDecoder.SLOT_SIZE + "))\n" +
                        "where\n" +
                        "  " + fk.getField().getName() + " <> 0\n" +
                        condWhere +
                        "order by\n" +
                        "  1";
            }
        }

        // Порядок следования записей важен даже при получении snapshot,
        // т.к. важно обеспечить правильный порядок вставки, например: триггер учитывает данные новой и ПРЕДЫДУЩЕЙ записи (см. например calc_SubjectOpr)
        return "select\n" +
                "  z_z_decode.own_slot,\n" +
                "  z_z_decode.ws_slot,\n" +
                "  z_z_decode.ws_id,\n" +
                "  (" + tableFrom.getName() + ".id - z_z_decode.own_slot * " + RefDecoder.SLOT_SIZE + ") as id_ws,\n" +
                "  " + tableFields + "\n" +
                "from\n" +
                "  " + tableFrom.getName() + "\n" +
                "  left join z_z_decode on (z_z_decode.table_name = '" + tableFrom.getName() + "' and z_z_decode.own_slot = (" + tableFrom.getName() + ".id / " + RefDecoder.SLOT_SIZE + "))\n" +
                condWhere +
                "order by\n" +
                "  " + tableFrom.getPrimaryKey().get(0).getName();

        //return "select " + tableFields + " from " + tableFrom.getName() + " where " + conditions + " order by " + tableFrom.getPrimaryKey().get(0).getName();

    /*
фильтр по автору
select
  z_z_decode.own_slot,
  z_z_decode.ws_slot,
  z_z_decode.ws_id,
  (lic.id - z_z_decode.own_slot * 1000000) as id_ws,
  lic.*
from
  lic
  left join z_z_decode on (z_z_decode.table_name = 'LIC' and z_z_decode.own_slot = (lic.id / 1000000))
order by
  rnn

     */
    }


}
