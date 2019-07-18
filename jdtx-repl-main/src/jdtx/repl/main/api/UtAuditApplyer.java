package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.joda.time.*;

import java.util.*;

/**
 * Применяет реплики
 */
public class UtAuditApplyer {

    private Db db;
    private IJdxDbStruct struct;

    //
    protected static Log log = LogFactory.getLog("jdtx");


    //
    public UtAuditApplyer(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
    }

    /**
     * Применить данные из dataReader на рабочей станции selfWsId
     */
    public void applyReplica(JdxReplicaReaderXml dataReader, IPublication publication, long selfWsId) throws Exception {
        log.info("applyReplica, self.WsId: " + selfWsId + ", replica.WsId: " + dataReader.getWsId() + ", replica.age: " + dataReader.getAge());

        //
        List<IJdxTable> tables = struct.getTables();
        int tIdx = 0;

        //
        DbUtils dbu = new DbUtils(db, struct);

        //
        IJdxDbStruct publicationData = publication.getData();

        //
        DbAuditTriggersManager triggersManager = new DbAuditTriggersManager(db);

        //
        IRefDecoder decoder = new RefDecoder(db, selfWsId);


        //
        db.startTran();
        try {
            triggersManager.setTriggersOff();

            //
            String tableName = dataReader.nextTable();
            while (tableName != null) {
                //log.debug("  table: " + tableName);

                // Поиск таблицы tableName в структуре, только в одну сторону (из-за зависимостей)
                int n = -1;
                for (int i = tIdx; i < tables.size(); i++) {
                    if (tables.get(i).getName().compareToIgnoreCase(tableName) == 0) {
                        n = i;
                        break;
                    }
                }
                if (n == -1) {
                    throw new XError("table [" + tableName + "] found in replica data, but not found in dbstruct");
                }
                //
                tIdx = n;
                IJdxTable table = tables.get(n);
                String idFieldName = table.getPrimaryKey().get(0).getName();

                // Поиск полей таблицы в публикации (поля берем именно из правил публикаций)
                IJdxTable publicationTable = publicationData.getTable(tableName);
                //
                if (publicationTable == null) {
                    log.info("  skip table: " + tableName);

                    //
                    tableName = dataReader.nextTable();

                    //
                    continue;
                }
/*
                String publicationFields = null;
                for (int i = 0; i < publicationData.size(); i++) {
                    JSONObject publicationTable = (JSONObject) publicationData.get(i);
                    String publicationTableName = (String) publicationTable.get("table");
                    if (table.getName().compareToIgnoreCase(publicationTableName) == 0) {
                        publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
                        break;
                    }
                }
*/

                // Перебираем записи
                Map recValues = dataReader.nextRec();
                long count = 0;
                while (recValues != null) {
                    // Подготовка полей записи в recValues
                    for (IJdxField publicationField : publicationTable.getFields()) {
                        String publicationFieldName = publicationField.getName();
                        IJdxField field = table.getField(publicationFieldName);

                        // Поле - BLOB?
                        if (getDataType(field.getDbDatatype()) == DataType.BLOB) {
                            String blobBase64 = (String) recValues.get(publicationFieldName);
                            byte[] blob = UtString.decodeBase64(blobBase64);
                            recValues.put(publicationFieldName, blob);
                            continue;
                        }

                        // Поле - дата/время?
                        if (getDataType(field.getDbDatatype()) == DataType.DATETIME) {
                            String valueStr = (String) recValues.get(publicationFieldName);
                            DateTime value = new DateTime(valueStr);
                            recValues.put(publicationFieldName, value);
                            continue;
                        }

                        // Поле - ссылка?
                        IJdxTable refTable = field.getRefTable();
                        if (field.isPrimaryKey() || refTable != null) {
                            // Ссылка
                            String refTableName;
                            if (field.isPrimaryKey()) {
                                refTableName = table.getName();
                            } else {
                                refTableName = refTable.getName();
                            }
                            JdxRef ref = JdxRef.parse((String) recValues.get(publicationFieldName));
                            if (ref.ws_id == -1) {
                                ref.ws_id = dataReader.getWsId();
                            }
                            // Перекодировка ссылки
                            long ref_own = decoder.get_id_own(refTableName, ref.ws_id, ref.id);
                            recValues.put(publicationFieldName, ref_own);
                            continue;
                        }

                        //
                        recValues.put(publicationFieldName, recValues.get(publicationFieldName));
                    }

                    // INS/UPD/DEL
                    String publicationFields = Publication.filedsToString(publicationTable.getFields());
                    int oprType = Integer.valueOf((String) recValues.get("Z_OPR"));
                    if (oprType == JdxOprType.OPR_INS) {
                        try {
                            dbu.insertRec(table.getName(), recValues, publicationFields, null);
                        } catch (Exception e) {
                            if (isPrimaryKeyError(e.getCause().getMessage())) {
                                dbu.updateRec(table.getName(), recValues, publicationFields, null);
                            } else {
                                throw (e);
                            }
                        }
                    } else if (oprType == JdxOprType.OPR_UPD) {
                        dbu.updateRec(table.getName(), recValues, publicationFields, null);
                    } else if (oprType == JdxOprType.OPR_DEL) {
                        dbu.deleteRec(table.getName(), (Long) recValues.get(idFieldName));
                    }

                    //
                    recValues = dataReader.nextRec();

                    //
                    count++;
                    if (count % 200 == 0) {
                        log.info("  table: " + tableName + ", " + count);
                    }
                }

                //
                log.info("  table: " + tableName + ", total: " + count);

                //
                tableName = dataReader.nextTable();
            }


            //
            triggersManager.setTriggersOn();


            //
            db.commit();


        } catch (Exception e) {
            if (!triggersManager.triggersIsOn()) {
                triggersManager.setTriggersOn();
            }

            //
            db.rollback();

            //
            throw e;
        }

    }

    private int getDataType(String dbDatatypeName) {
        switch (dbDatatypeName) {
            case "SMALLINT":
                return DataType.INT;
            case "LONGINT":
            case "INTEGER":
                return DataType.LONG;
            case "FLOAT":
            case "NUMERIC":
            case "DOUBLE PRECISION":
                return DataType.DOUBLE;
            case "DATE":
            case "TIME":
            case "TIMESTAMP":
                return DataType.DATETIME;
            case "CHAR":
            case "VARCHAR":
                return DataType.STRING;
            case "---":
                return DataType.BOOLEAN;
            case "BLOB SUB_TYPE 0":
                return DataType.BLOB;
            default:
                return DataType.OBJECT;
        }
    }

    private boolean isPrimaryKeyError(String message) {
        return message.contains("violation of PRIMARY or UNIQUE KEY constraint");
    }


}
