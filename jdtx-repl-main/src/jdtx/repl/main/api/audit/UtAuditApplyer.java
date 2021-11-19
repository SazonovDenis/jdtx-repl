package jdtx.repl.main.api.audit;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.filter.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 * Применяет реплики
 */
public class UtAuditApplyer {

    private Db db;
    private IJdxDbStruct struct;
    private long wsId;
    public JdxReplWs jdxReplWs;


    //
    protected static Log log = LogFactory.getLog("jdtx.AuditApplyer");


    //
    public UtAuditApplyer(Db db, IJdxDbStruct struct, long wsId) throws Exception {
        this.db = db;
        this.struct = struct;
        this.wsId = wsId;
    }

    public void applyReplica(IReplica replica, IPublicationRuleStorage publicationIn, Map<String, String> filterParams, boolean forceApply_ignorePublicationRules, long commitPortionMax) throws Exception {
        //
        InputStream inputStream = null;
        try {
            // Распакуем XML-файл из Zip-архива
            inputStream = JdxReplicaReaderXml.createInputStreamData(replica);

            //
            JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

            //
            try {
                applyReplicaReader(replicaReader, publicationIn, filterParams, forceApply_ignorePublicationRules, commitPortionMax);
            } catch (Exception e) {
                if (e instanceof JdxForeignKeyViolationException) {
                    File replicaRepairFile = jdxReplWs.handleFailedInsertUpdateRef((JdxForeignKeyViolationException) e);
                    // todo крайне криво - транзакция же ждет!!!
                    boolean autoUseRepairReplica = jdxReplWs.appCfg.getValueBoolean("autoUseRepairReplica");
                    if (autoUseRepairReplica) {
                        log.warn("==================================");
                        log.warn("==================================");
                        log.warn("==================================");
                        log.warn("Восстанавливаем записи из временной реплики: " + replicaRepairFile.getAbsolutePath());
                        ReplicaUseResult useResult = jdxReplWs.useReplicaFile(replicaRepairFile);
                        if (!useResult.replicaUsed) {
                            log.error("Временная реплика не использована: " + replicaRepairFile.getAbsolutePath());
                        }
                        if (replicaRepairFile.delete()) {
                            log.info("Файл временной реплики удален");
                        } else {
                            log.error("Файл временной реплики не удалось удалить");
                        }
                    } else {
                        log.warn("autoUseRepairReplica: " + autoUseRepairReplica);
                    }
                }
                throw (e);
            }
        } finally {
            // Закроем читателя Zip-файла
            if (inputStream != null) {
                inputStream.close();
            }
        }

    }

    /**
     * Применить данные из dataReader на рабочей станции selfWsId
     */
    public void applyReplicaReader(JdxReplicaReaderXml dataReader, IPublicationRuleStorage publicationRules, Map<String, String> filterParams, boolean forceApply_ignorePublicationRules, long portionMax) throws Exception {
        log.info("applyReplica, self.WsId: " + wsId + ", replica.WsId: " + dataReader.getWsId() + ", replica.age: " + dataReader.getAge());

        //
        List<IJdxTable> tables = struct.getTables();
        int tIdx = 0;

        //
        JdxDbUtils dbu = new JdxDbUtils(db, struct);

        //
        AuditDbTriggersManager triggersManager = new AuditDbTriggersManager(db);

        //
        IJdxDataSerializer dataSerializer = new JdxDataSerializerDecode(db, wsId);

        //
        db.startTran();
        try {
            // На время применения аудита нужно выключать триггера
            triggersManager.setTriggersOff();


            // Тут копим задания на DELETE для всей реплики, их выполняем вторым проходом,
            // см. подробности в UtAuditApplyer.md
            Map<String, Collection<Long>> delayedDeleteTask = new HashMap<>();

            //
            String readerTableName = dataReader.nextTable();
            String readerTableNamePrior = "";
            long countPortion = 0;
            long count = 0;
            while (readerTableName != null) {
                // Поиск таблицы readerTableName в структуре, только в одну сторону (из-за зависимостей)
                int n = -1;
                for (int i = tIdx; i < tables.size(); i++) {
                    if (tables.get(i).getName().compareToIgnoreCase(readerTableName) == 0) {
                        n = i;
                        break;
                    }
                }
                if (n == -1) {
                    // Для справки/отладки - структуры в файл
                    jdxReplWs.debugDumpStruct("7.");
                    //
                    throw new XError("table [" + readerTableName + "] found in replica data, but not found in dbstruct");
                }
                //
                tIdx = n;
                IJdxTable table = tables.get(n);
                String pkFieldName = table.getPrimaryKey().get(0).getName();
                String tableName = table.getName();

                //
                IRecordFilter recordFilter = null;

                // Поиск таблицы и ее полей в публикации (поля берем именно из правил публикаций)
                IPublicationRule publicationRuleTable;
                if (forceApply_ignorePublicationRules) {
                    // Таблицу и ее поля берем из актуальной структуры БД
                    publicationRuleTable = new PublicationRule(struct.getTable(readerTableName));
                    log.info("  force apply table: " + readerTableName + ", ignore publication rules");

                    //
                    recordFilter = new RecordFilterTrue();
                } else {
                    // Таблицу и ее поля берем именно из правил публикаций
                    publicationRuleTable = publicationRules.getPublicationRule(readerTableName);
                    if (publicationRuleTable == null) {
                        log.info("  skip table: " + readerTableName + ", not found in publication");

                        //
                        readerTableName = dataReader.nextTable();

                        //
                        continue;
                    }

                    //
                    recordFilter = new RecordFilter(publicationRuleTable, tableName, filterParams);
                }

                // Тут будем копить задания на DELETE для таблицы tableName, их выполняем вторым проходом.
                Collection<Long> delayedDeleteTaskForTable = delayedDeleteTask.get(tableName);
                if (delayedDeleteTaskForTable == null) {
                    delayedDeleteTaskForTable = new ArrayList<>();
                    delayedDeleteTask.put(tableName, delayedDeleteTaskForTable);
                }

                // Перебираем записи
                countPortion = 0;
                if (readerTableName.compareToIgnoreCase(readerTableNamePrior) == 0) {
                    log.info("next portion: " + readerTableName);
                } else {
                    count = 0;
                }
                readerTableNamePrior = readerTableName;

                //
                String publicationFieldsName = UtJdx.fieldsToString(publicationRuleTable.getFields());
                dataSerializer.setTable(table, publicationFieldsName);

                //
                Map<String, String> recValuesStr = dataReader.nextRec();
                while (recValuesStr != null) {

                    // Перебираем записи, пропускаем те, которые не подходят под наши входящие фильтры publicationRules
                    if (recordFilter.isMach(recValuesStr)) {

                        // Обеспечим не слишком огромные порции коммитов
                        if (portionMax != 0 && countPortion >= portionMax) {
                            countPortion = 0;
                            //
                            db.commit();
                            db.startTran();
                            //
                            log.info("  table: " + readerTableName + ", " + count + ", commit/startTran");
                        }
                        countPortion++;


                        // Подготовка recParams для записи в БД - десериализация значений
                        Map<String, Object> recParams = dataSerializer.prepareValues(recValuesStr);


                        // Выполняем INS/UPD/DEL
                        // Очень важно взять поля для обновления (publicationFieldsName) именно из правил публикации,
                        // а не все что есть в физической  таблице, т.к. именно по этим правилам готовилась реплика на сервере,
                        // при этом мог использоваться НЕ ПОЛНЫЙ набор полей. Из-за такого пропуска полей,
                        // при получении на рабочей станции СВОЕЙ реплики и попытке обновить ВСЕ поля,
                        // пропущенные поля станут null. На ДРУГИХ филиалах это не страшно, а на НАШЕЙ - данные затрутся.
                        // (Неполный набр полей используется, например, если на филиалы НЕ отправляются данные из справочников,
                        // на которые ссылается рассматриваемая таблица, например: "примечания, сделанные пользователем":
                        // сами примечания отправляем, а ССЫЛКИ на пользователей придется пропустить).
                        int oprType = UtJdxData.intValueOf(recValuesStr.get(UtJdx.XML_FIELD_OPR_TYPE));
                        long recId = (Long) recParams.get(pkFieldName);
                        if (oprType == JdxOprType.OPR_INS) {
                            try {
                                // Отменим удаление этой записи на втором проходе
                                delayedDeleteTaskForTable.remove(recId);
                                //
                                insertOrUpdate(dbu, tableName, recParams, publicationFieldsName);
                            } catch (Exception e) {
                                if (UtDbErrors.errorIs_ForeignKeyViolation(e)) {
                                    log.error(e.getMessage());
                                    log.error("table: " + tableName);
                                    log.error("oprType: " + oprType);
                                    log.error("recParams: " + recParams);
                                    log.error("recValuesStr: " + recValuesStr);
                                    //
                                    JdxForeignKeyViolationException ee = new JdxForeignKeyViolationException(e);
                                    ee.recParams = recParams;
                                    ee.recValues = recValuesStr;
                                    // todo вообще, костыль страшнейший, сделан для пропуска неуместных реплик,
                                    // которые просочились на станцию из-за кривых настроек фильтров.
                                    // todo Убрать, когда будут сделана фильтрация по ссылкам!!!
                                    boolean skipForeignKeyViolationIns = jdxReplWs.appCfg.getValueBoolean("skipForeignKeyViolationIns");
                                    if (skipForeignKeyViolationIns) {
                                        log.error("skipForeignKeyViolationIns: " + skipForeignKeyViolationIns);
                                    } else {
                                        throw (ee);
                                    }
                                } else {
                                    throw (e);
                                }
                            }

                        } else if (oprType == JdxOprType.OPR_UPD) {
                            try {
                                dbu.updateRec(tableName, recParams, publicationFieldsName, null);
                            } catch (Exception e) {
                                if (UtDbErrors.errorIs_ForeignKeyViolation(e)) {
                                    log.error(e.getMessage());
                                    log.error("table: " + tableName);
                                    log.error("oprType: " + oprType);
                                    log.error("recParams: " + recParams);
                                    log.error("recValuesStr: " + recValuesStr);
                                    //
                                    JdxForeignKeyViolationException ee = new JdxForeignKeyViolationException(e);
                                    ee.recParams = recParams;
                                    ee.recValues = recValuesStr;
                                    // todo вообще, костыль страшнейший, сделан для пробуска неуместных реплик,
                                    // которые просочились на станцию из-за кривых настроек фильтров.
                                    // todo Убрать, когда будут сделана фильтрация по ссылкам!!!
                                    boolean skipForeignKeyViolationUpd = jdxReplWs.appCfg.getValueBoolean("skipForeignKeyViolationUpd");
                                    if (skipForeignKeyViolationUpd) {
                                        log.error("skipForeignKeyViolationUpd: " + skipForeignKeyViolationUpd);
                                    } else {
                                        throw (ee);
                                    }
                                } else {
                                    throw (e);
                                }
                            }
                        } else if (oprType == JdxOprType.OPR_DEL) {
                            // Отложим удаление на второй проход
                            delayedDeleteTaskForTable.add(recId);
                        }
                    }

                    //
                    recValuesStr = dataReader.nextRec();

                    //
                    count = count + 1;
                    if (count % 1000 == 0) {
                        log.info("  table: " + readerTableName + ", " + count);
                    }
                }


                //
                log.info("  done: " + readerTableName + ", total: " + count);


                //
                readerTableName = dataReader.nextTable();
            }


            // Ворой проход - выполнение удаления отложенных
            // log.info("applyReplica, delayed delete");
            for (int i = tables.size() - 1; i >= 0; i--) {
                IJdxTable table = tables.get(i);
                String tableName = table.getName();

                //
                Collection<Long> delayedDeleteTaskForTable = delayedDeleteTask.get(tableName);
                if (delayedDeleteTaskForTable == null) {
                    continue;
                }
                if (delayedDeleteTaskForTable.size() == 0) {
                    continue;
                }

                //
                List<Long> failedDeleteList = new ArrayList<>();
                count = 0;
                for (Long recId : delayedDeleteTaskForTable) {
                    try {
                        dbu.deleteRec(tableName, recId);
                        count = count + 1;
                    } catch (Exception e) {
                        if (UtDbErrors.errorIs_ForeignKeyViolation(e)) {
                            // Пропустим реплику, а ниже - выдадим в исходящую очередь наш вариант удаляемой записи
                            log.info("  table: " + tableName + ", fail to delete: " + failedDeleteList.size());
                            failedDeleteList.add(recId);
                        } else {
                            log.error("table: " + tableName + ", table.id: " + recId);
                            //
                            throw (e);
                        }
                    }

                    //
                    count = count + 1;
                    if (count % 200 == 0) {
                        log.info("  table delete: " + tableName + ", " + count);
                    }

                }

                //
                log.info("  done delete: " + tableName + ", total: " + count);

                // Обратка от удалений, которые не удалось выполнить - создаем реплики на вставку (выдадим в исходящую очередь наш вариант удаляемой записи),
                // чтобы те, кто уже удалил - раскаялись и вернули все назад, по данныим из НАШИХ реплик.
                // todo крайне криво - транзакция же ждет!!!
                // todo: этот метод В КОНТЕКТСЕ ТРАНЗАКЦИИ возится с какими то файлами и проч... - нежелательно
                // todo: комит тут внутри, а контекст с этим методом createTableReplicaByIdList() - снаружи
                if (failedDeleteList.size() != 0) {
                    log.info("  failed delete: " + tableName + ", count: " + failedDeleteList.size());
                    //
                    jdxReplWs.createSnapshotByIdListIntoQueOut(tableName, failedDeleteList);
                    //
                    log.info("  failed delete: " + tableName + ", snapshot done");
                    //
                    failedDeleteList.clear();
                }

            }


            // После применения аудита можно снова включать триггера
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


    public static Long insertOrUpdate(JdxDbUtils dbu, String tableName, Map<String, Object> recParams, String publicationFields) throws Exception {
        Long id = null;

        try {
            id = dbu.insertRec(tableName, recParams, publicationFields, null);
        } catch (Exception e) {
            if (UtDbErrors.errorIs_PrimaryKeyError(e)) {
                dbu.updateRec(tableName, recParams, publicationFields, null);
            } else {
                throw e;
            }
        }

        return id;
    }

    public static int getDataType(String dbDatatypeName) {
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
