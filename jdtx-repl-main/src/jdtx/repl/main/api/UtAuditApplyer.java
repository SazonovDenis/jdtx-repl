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
    JdxReplWs jdxReplWs;


    //
    protected static Log log = LogFactory.getLog("jdtx.AuditApplyer");


    //
    public UtAuditApplyer(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
    }

    /**
     * Применить данные из dataReader на рабочей станции selfWsId
     */
    public void applyReplica(JdxReplicaReaderXml dataReader, IPublicationStorage publication, boolean forceApply_ignorePublicationRules, long selfWsId, long portionMax) throws Exception {
        log.info("applyReplica, self.WsId: " + selfWsId + ", replica.WsId: " + dataReader.getWsId() + ", replica.age: " + dataReader.getAge());

        //
        List<IJdxTable> tables = struct.getTables();
        int tIdx = 0;

        //
        JdxDbUtils dbu = new JdxDbUtils(db, struct);

        //
        DbAuditTriggersManager triggersManager = new DbAuditTriggersManager(db);

        //
        IRefDecoder decoder = new RefDecoder(db, selfWsId);

        //
        List<Long> failedDeleteId = new ArrayList<>();

        //
        db.startTran();
        try {
            triggersManager.setTriggersOff();

            //
            String tableName = dataReader.nextTable();
            String tableNamePrior = "";
            long countPortion = 0;
            long count = 0;
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

                // Поиск таблицы и ее полей в публикации (поля берем именно из правил публикаций)
                IPublicationRule publicationTable;
                if (forceApply_ignorePublicationRules) {
                    // Таблицу и ее поля берем из структуры
                    publicationTable = new PublicationRule(struct.getTable(tableName));
                    //publicationTable = struct.getTable(tableName);
                    log.info("  force apply table: " + tableName + ", ignore publication rules");
                } else {
                    // Таблицу и ее поля берем именно из правил публикаций
                    publicationTable = publication.getPublicationRule(tableName);
                    if (publicationTable == null) {
                        log.info("  skip table: " + tableName + ", not found in publication");

                        //
                        tableName = dataReader.nextTable();

                        //
                        continue;
                    }
                }
/*
                String publicationFields = null;
                for (int i = 0; i < publicationRules.size(); i++) {
                    JSONObject publicationTable = (JSONObject) publicationRules.get(i);
                    String publicationTableName = (String) publicationTable.get("table");
                    if (table.getName().compareToIgnoreCase(publicationTableName) == 0) {
                        publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
                        break;
                    }
                }
*/

                // Перебираем записи
                countPortion = 0;
                if (tableName.compareToIgnoreCase(tableNamePrior) == 0) {
                    log.info("next portion: " + tableName);
                } else {
                    count = 0;
                }
                tableNamePrior = tableName;

                Map recValues = dataReader.nextRec();
                while (recValues != null) {
                    // Обеспечим не слишком огромные порции коммитов
                    if (portionMax != 0 && countPortion >= portionMax) {
                        countPortion = 0;
                        //
                        db.commit();
                        db.startTran();
                        //
                        log.info("  table: " + tableName + ", " + count + ", commit/startTran");
                    }
                    countPortion++;

                    // Подготовка recParams - значений полей для записи в БД
                    Map recParams = new HashMap();
                    for (IJdxField publicationField : publicationTable.getFields()) {
                        String publicationFieldName = publicationField.getName();
                        IJdxField field = table.getField(publicationFieldName);

                        // Поле - BLOB?
                        if (getDataType(field.getDbDatatype()) == DataType.BLOB) {
                            String blobBase64 = (String) recValues.get(publicationFieldName);
                            byte[] blob = UtString.decodeBase64(blobBase64);
                            recParams.put(publicationFieldName, blob);
                            continue;
                        }

                        // Поле - дата/время?
                        if (getDataType(field.getDbDatatype()) == DataType.DATETIME) {
                            String valueStr = (String) recValues.get(publicationFieldName);
                            DateTime value = new DateTime(valueStr);
                            recParams.put(publicationFieldName, value);
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
                            recParams.put(publicationFieldName, ref_own);
                            continue;
                        }

                        // Просто поле, без изменений
                        recParams.put(publicationFieldName, recValues.get(publicationFieldName));
                    }

                    // Выполняем INS/UPD/DEL
                    String publicationFields = PublicationStorage.filedsToString(publicationTable.getFields());
                    int oprType = Integer.valueOf((String) recValues.get("Z_OPR"));
                    if (oprType == JdxOprType.OPR_INS) {
                        try {
                            insertOrUpdate(dbu, table.getName(), recParams, publicationFields);
                        } catch (Exception e) {
                            if (JdxUtils.errorIs_ForeignKeyViolation(e)) {
                                log.error("recParams: " + recParams);
                                log.error("recValues: " + recValues);
                                //
                                JdxForeignKeyViolationException ee = new JdxForeignKeyViolationException(e);
                                ee.recParams = recParams;
                                ee.recValues = recValues;
                                throw (ee);
                            } else {
                                throw (e);
                            }
                        }

                    } else if (oprType == JdxOprType.OPR_UPD) {
                        try {
                            dbu.updateRec(table.getName(), recParams, publicationFields, null);
                        } catch (Exception e) {
                            if (JdxUtils.errorIs_ForeignKeyViolation(e)) {
                                log.error("recParams: " + recParams);
                                log.error("recValues: " + recValues);
                                //
                                JdxForeignKeyViolationException ee = new JdxForeignKeyViolationException(e);
                                ee.recParams = recParams;
                                ee.recValues = recValues;
                                throw (ee);
                            } else {
                                throw (e);
                            }
                        }
                    } else if (oprType == JdxOprType.OPR_DEL) {
                        try {
                            dbu.deleteRec(table.getName(), (Long) recParams.get(idFieldName));
                        } catch (Exception e) {
                            if (JdxUtils.errorIs_ForeignKeyViolation(e)) {
                                // Пропустим реплику, выдадим в исходящую очередь наш вариант удаляемой записи
                                // todo: этот метод В КОНТЕКТСЕ ТРАНЗАКЦИИ возится с какими то файлами и проч... - нежелательно
                                failedDeleteId.add((Long) recParams.get(idFieldName));
                            } else {
                                log.error("recParams: " + recParams);
                                log.error("recValues: " + recValues);
                                //
                                throw (e);
                            }
                        }
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
                log.info("  done: " + tableName + ", total: " + count);


                // Обратка от удалений, которые не удалось выполнить - создаем реплики на вставку,
                // чтобы те, куто уже удалил - раскаялись и вернули все назад.
                if (failedDeleteId.size() != 0) {
                    log.info("  table: " + tableName + ", fail to delete: " + failedDeleteId.size());

                    // todo: этот метод В КОНТЕКТСЕ ТРАНЗАКЦИИ возится с какими то файлами и проч... - нежелательно
                    // todo: комит тут внутри, а контекст с этим методом createTableReplicaByIdList() - снаружи
                    jdxReplWs.createTableReplicaByIdList(tableName, failedDeleteId);
                }


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

    private void insertOrUpdate(JdxDbUtils dbu, String tableName, Map recParams, String publicationFields) throws Exception {
        try {
            dbu.insertRec(tableName, recParams, publicationFields, null);
        } catch (Exception e) {
            if (JdxUtils.errorIs_PrimaryKeyError(e)) {
                dbu.updateRec(tableName, recParams, publicationFields, null);
            } else {
                throw e;
            }
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


}
