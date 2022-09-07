package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.data_binder.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.ref_manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 * Извлекатель всех данных из таблицы
 */
public class UtDataSelector {

    private Db db;
    private IJdxDbStruct struct;
    private IJdxDataSerializer dataSerializer;

    private long MAX_SNAPSHOT_RECS = 5000;

    //
    protected static Log log = LogFactory.getLog("jdtx.DataSelector");


    /**
     */
    public UtDataSelector(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
        RefManagerService refManagerService = db.getApp().service(RefManagerService.class);
        this.dataSerializer = refManagerService.createDataSerializer();
    }

    /**
     * Извлекатет все данные по правилу publicationRule (из таблицы).
     * Если в таблице есть ссылка на саму себя (полк типа ParentId), процедура обязана обеспечить правильную последовательность записей.
     */
    public void readAllRecords(IPublicationRule publicationRule, JdxReplicaWriterXml dataWriter) throws Exception {
        // DbQuery, содержащие все данные из таблицы tableName
        IJdxDataBinder rsTableLog = selectAllRecords(publicationRule);

        //
        try {
            String tableName = publicationRule.getTableName();
            String tableFields = UtJdx.fieldsToString(publicationRule.getFields());
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
        // Таблица и поля в Serializer-е
        IJdxTable table = struct.getTable(tableName);
        dataSerializer.setTable(table, tableFields);

        // Таблица во Writer-е
        dataWriter.startTable(tableName);

        // Данные помещаем в dataWriter
        long count = 0;
        long countPortion = 0;
        while (!data.eof()) {
            Map<String, Object> values = data.getValues();

            // Обеспечим не слишком огромные порции данных
            if (countPortion >= MAX_SNAPSHOT_RECS) {
                countPortion = 0;
                dataWriter.startTable(tableName);
            }
            countPortion++;

            // Добавляем запись
            dataWriter.appendRec();

            // Тип операции
            dataWriter.writeOprType(JdxOprType.INS);

            // Тело записи
            Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
            UtXml.recToWriter(valuesStr, tableFields, dataWriter);

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

    IJdxDataBinder selectAllRecords(IPublicationRule publicationRule) throws Exception {
        //
        String sql = getSqlAllRecords(publicationRule);

        //
        DbQuery query = db.openSql(sql);

        //
        return new JdxDataBinder_DbQuery(query);
    }

    String getSqlAllRecords(IPublicationRule publicationRule) {
        String tableName = publicationRule.getTableName();
        IJdxTable tableFrom = struct.getTable(tableName);
        //
        String tableFields = UtJdx.fieldsToString(publicationRule.getFields(), tableFrom.getName() + ".");
        //
        String condWhere = "";

        // Таблица древовидная (имеет ссылки на саму себя)?
        for (IJdxForeignKey fk : tableFrom.getForeignKeys()) {
            if (fk.getTable().getName().equals(tableFrom.getName())) {
                // todo: Пока так реализуем правильную последовательность записей (если есть ссылка на самого себя)
                return "select\n" +
                        "  0 as dummySortField, \n" +
                        "  " + tableFields + "\n" +
                        "from\n" +
                        "  " + tableFrom.getName() + "\n" +
                        "  left join " + UtJdx.SYS_TABLE_PREFIX + "decode on (" + UtJdx.SYS_TABLE_PREFIX + "decode.table_name = '" + tableFrom.getName() + "' and " + UtJdx.SYS_TABLE_PREFIX + "decode.own_slot = (" + tableFrom.getName() + ".id / " + RefManager_Decode.SLOT_SIZE + "))\n" +
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
                        "  left join " + UtJdx.SYS_TABLE_PREFIX + "decode on (" + UtJdx.SYS_TABLE_PREFIX + "decode.table_name = '" + tableFrom.getName() + "' and " + UtJdx.SYS_TABLE_PREFIX + "decode.own_slot = (" + tableFrom.getName() + ".id / " + RefManager_Decode.SLOT_SIZE + "))\n" +
                        "where\n" +
                        "  " + fk.getField().getName() + " <> 0\n" +
                        condWhere +
                        "order by\n" +
                        "  1";
            }
        }

        // Порядок следования записей важен даже при получении snapshot,
        // т.к. важно обеспечить правильный порядок вставки, например: триггер учитывает данные новой и ПРЕДЫДУЩЕЙ записи (см. например в PS: calc_SubjectOpr)
        return "select\n" +
                "  " + UtJdx.SYS_TABLE_PREFIX + "decode.own_slot,\n" +
                "  " + UtJdx.SYS_TABLE_PREFIX + "decode.ws_slot,\n" +
                "  " + UtJdx.SYS_TABLE_PREFIX + "decode.ws_id,\n" +
                "  (" + tableFrom.getName() + ".id - " + UtJdx.SYS_TABLE_PREFIX + "decode.own_slot * " + RefManager_Decode.SLOT_SIZE + ") as id_ws,\n" +
                "  " + tableFields + "\n" +
                "from\n" +
                "  " + tableFrom.getName() + "\n" +
                "  left join " + UtJdx.SYS_TABLE_PREFIX + "decode on (" + UtJdx.SYS_TABLE_PREFIX + "decode.table_name = '" + tableFrom.getName() + "' and " + UtJdx.SYS_TABLE_PREFIX + "decode.own_slot = (" + tableFrom.getName() + ".id / " + RefManager_Decode.SLOT_SIZE + "))\n" +
                condWhere +
                "order by\n" +
                "  " + tableFrom.getPrimaryKey().get(0).getName();
    }


}
