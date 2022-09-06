package jdtx.repl.main.api.audit;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.ref_manager.*;
import jdtx.repl.main.api.filter.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.joda.time.*;

import java.io.*;
import java.util.*;

/**
 * Применяет реплики
 */
public class UtAuditApplyer {

    private Db db;
    private IJdxDbStruct struct;
    public JdxReplWs jdxReplWs;

    //
    protected static Log log = LogFactory.getLog("jdtx.AuditApplyer");


    //
    public UtAuditApplyer(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
    }

    public void applyReplica(IReplica replica, IPublicationRuleStorage publicationIn, Map<String, String> filterParams, boolean forceApply_ignorePublicationRules, long commitPortionMax) throws Exception {
        //
        JdxReplicaFileInputStream inputStream = null;
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
                    // todo крайне криво - транзакция же ждет!!!
                    log.warn("==========");
                    JdxForeignKeyViolationException eFk = (JdxForeignKeyViolationException) e;
                    log.warn("Обработка ошибки foreignKey: " + e.getMessage());
                    log.warn("table: " + eFk.tableName);
                    log.warn("oprType: " + eFk.oprType);
                    log.warn("recParams: " + eFk.recParams);
                    log.warn("recValuesStr: " + eFk.recValues);
                    File replicaRepairFile = jdxReplWs.handleFailedInsertUpdateRef(eFk);
                    boolean autoUseRepairReplica = jdxReplWs.appCfg.getValueBoolean("autoUseRepairReplica");
                    if (autoUseRepairReplica) {
                        log.warn("Восстанавливаем записи из результатов поиска: " + replicaRepairFile.getAbsolutePath());
                        ReplicaUseResult useResult = jdxReplWs.useReplicaFile(replicaRepairFile);
                        if (!useResult.replicaUsed) {
                            log.error("Реплика с результатами поиска не использована: " + replicaRepairFile.getAbsolutePath());
                        }
                        if (!replicaRepairFile.delete()) {
                            log.error("Файл реплики с результатами поиска не удалось удалить");
                        }
                    } else {
                        log.warn("Обработка ошибки не выполнена, autoUseRepairReplica: " + autoUseRepairReplica);
                    }
                    log.warn("----------");
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
        log.info("applyReplica, replica.WsId: " + dataReader.getWsId() + ", replica.no: " + dataReader.getNo() + ", replica.age: " + dataReader.getAge());

        //
        List<IJdxTable> tables = struct.getTables();
        int tIdx = 0;

        //
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        IDbErrors dbErrors = db.getApp().service(DbToolsService.class).getDbErrors(db);

        //
        AuditDbTriggersManager triggersManager = new AuditDbTriggersManager(db);

        //
        RefManagerService refManagerService = db.getApp().service(RefManagerService.class);
        IJdxDataSerializer dataSerializer = refManagerService.getJdxDataSerializer();

        //
        SelfAuditDtComparer selfAuditDtComparer = new SelfAuditDtComparer(db);
        int replicaType = dataReader.getReplicaType();
        DateTime replicaDtTo = dataReader.getDtTo();

        //
        db.startTran();
        try {
            // На время применения аудита нужно выключать триггера
            triggersManager.setTriggersOff();


            // Тут копим задания на DELETE для всей реплики, их выполняем вторым проходом,
            // Логику работы с delayedDeleteTask см. разделе "Взаимные зависимости в рамках одной реплики" (jdtx/repl/main/api/audit/UtAuditApplyer.md)
            Map<String, Collection<Long>> delayedDeleteTask = new HashMap<>();

            //
            long fileSize = dataReader.getInputStream().getSize();
            long countPortion = 0;
            long count = 0;
            //
            String readerTableName = dataReader.nextTable();
            String readerTableNamePrior = "";
            //
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

                // Для проверки, что записи, обновляемые из текущей реплики НЕ были ещё раз изменены на рабочей станции,
                // после того, как текущая реплика была отправлена. Актуально при применении СОБСТВЕННЫХ реплик.
                // Также возможно, если кто-то другой отредактировал запись, которую недавно редавтировали мы -
                // тогда selfAuditDtComparer решает, какая запись будет считаться последней.
                if (replicaType != JdxReplicaType.SNAPSHOT) {
                    // Предполагается, что SNAPSHOT или IDE_MERGE просто так не присылают,
                    // значит дело серьезное и нужно обязательно применить.
                    selfAuditDtComparer.readSelfAuditData(tableName, replicaDtTo);
                }

                // Тут будем копить задания на DELETE для таблицы tableName, их выполняем вторым проходом.
                Collection<Long> delayedDeleteTaskForTable = delayedDeleteTask.get(tableName);
                if (delayedDeleteTaskForTable == null) {
                    delayedDeleteTaskForTable = new ArrayList<>();
                    delayedDeleteTask.put(tableName, delayedDeleteTaskForTable);
                }

                // Перебираем записи
                if (readerTableName.compareToIgnoreCase(readerTableNamePrior) != 0) {
                    // Новая таблица - сброс счетчика записей
                    count = 0;
                } else {
                    // Продолжение этой же таблицы новой порцией
                    log.info("next portion: " + readerTableName);
                }
                readerTableNamePrior = readerTableName;

                // Очень важно взять поля для обновления (publicationFields) именно из правил публикации,
                // а не все что есть в физической  таблице, т.к. именно по этим правилам готовилась реплика на сервере,
                // при этом мог использоваться НЕ ПОЛНЫЙ набор полей. Из-за такого пропуска полей,
                // при получении на рабочей станции СВОЕЙ реплики и попытке обновить ВСЕ поля,
                // пропущенные поля станут null. На ДРУГИХ филиалах это не страшно, а на НАШЕЙ - данные затрутся.
                // (Неполный набр полей используется, например, если на филиалы НЕ отправляются данные из справочников,
                // на которые ссылается рассматриваемая таблица, например: "примечания, сделанные пользователем":
                // сами примечания отправляем, а ССЫЛКИ на пользователей придется пропустить).
                String publicationFields = UtJdx.fieldsToString(publicationRuleTable.getFields());

                // Таблица и поля в Serializer-е
                dataSerializer.setTable(table, publicationFields);

                // Перебираем записи
                Map<String, String> recValuesStr = dataReader.nextRec();
                while (recValuesStr != null) {

                    // Перебираем записи, пропускаем те, которые не подходят под наши входящие фильтры publicationRules
                    if (recordFilter.isMach(recValuesStr)) {

                        // Подготовка recParams для записи в БД - десериализация значений
                        Map<String, Object> recParams = dataSerializer.prepareValues(recValuesStr);


                        // Выполняем INS/UPD/DEL
                        JdxOprType oprType = JdxOprType.valueOfStr(recValuesStr.get(UtJdx.XML_FIELD_OPR_TYPE));
                        long recId = (Long) recParams.get(pkFieldName);
                        if (oprType == JdxOprType.INS) {
                            // Отменим удаление этой записи на втором проходе
                            delayedDeleteTaskForTable.remove(recId);

                            // Проверяем, что обновляемая запись НЕ была ещё раз изменена
                            if (selfAuditDtComparer.isSelfAuditAgeAboveReplicaAge(recId)) {
                                log.info("Self audit age > replica age, record skipped, oprType: " + oprType + ", table: " + tableName + ", id: " + recId);
                            } else {
                                try {
                                    //
                                    dbu.insertOrUpdate(tableName, recParams, publicationFields);
                                } catch (Exception e) {
                                    if (dbErrors.errorIs_ForeignKeyViolation(e)) {
                                        JdxForeignKeyViolationException ee = new JdxForeignKeyViolationException(e);
                                        ee.tableName = tableName;
                                        ee.oprType = oprType;
                                        ee.recParams = recParams;
                                        ee.recValues = recValuesStr;
                                        // todo вообще, костыль страшнейший, сделан для пропуска неуместных реплик,
                                        // которые просочились на станцию из-за кривых настроек фильтров.
                                        // todo Убрать skipForeignKeyViolationIns, когда будут сделана фильтрация по ссылкам!!!
                                        boolean skipForeignKeyViolationIns = jdxReplWs.appCfg.getValueBoolean("skipForeignKeyViolationIns");
                                        if (skipForeignKeyViolationIns) {
                                            log.error(e.getMessage());
                                            log.error("table: " + tableName);
                                            log.error("oprType: " + oprType);
                                            log.error("recParams: " + recParams);
                                            log.error("recValuesStr: " + recValuesStr);
                                            log.error("skipForeignKeyViolationIns: " + skipForeignKeyViolationIns);
                                        } else {
                                            throw (ee);
                                        }
                                    } else {
                                        throw (e);
                                    }
                                }
                            }

                        } else if (oprType == JdxOprType.UPD) {
                            // Проверяем, что обновляемая запись НЕ была ещё раз изменена
                            if (selfAuditDtComparer.isSelfAuditAgeAboveReplicaAge(recId)) {
                                log.info("Self audit age > replica age, record skipped, oprType: " + oprType + ", table: " + tableName + ", id: " + recId);
                            } else {
                                try {
                                    dbu.updateRec(tableName, recParams, publicationFields, null);
                                } catch (Exception e) {
                                    if (dbErrors.errorIs_ForeignKeyViolation(e)) {
                                        JdxForeignKeyViolationException ee = new JdxForeignKeyViolationException(e);
                                        ee.tableName = tableName;
                                        ee.oprType = oprType;
                                        ee.recParams = recParams;
                                        ee.recValues = recValuesStr;
                                        // todo вообще, костыль страшнейший, сделан для пробуска неуместных реплик,
                                        // которые просочились на станцию из-за кривых настроек фильтров.
                                        // todo Убрать skipForeignKeyViolationIns, когда будут сделана фильтрация по ссылкам!!!
                                        boolean skipForeignKeyViolationUpd = jdxReplWs.appCfg.getValueBoolean("skipForeignKeyViolationUpd");
                                        if (skipForeignKeyViolationUpd) {
                                            log.error(e.getMessage());
                                            log.error("table: " + tableName);
                                            log.error("oprType: " + oprType);
                                            log.error("recParams: " + recParams);
                                            log.error("recValuesStr: " + recValuesStr);
                                            log.error("skipForeignKeyViolationUpd: " + skipForeignKeyViolationUpd);
                                        } else {
                                            throw (ee);
                                        }
                                    } else {
                                        throw (e);
                                    }
                                }
                            }
                        } else if (oprType == JdxOprType.DEL) {
                            // Отложим удаление на второй проход
                            delayedDeleteTaskForTable.add(recId);
                        }
                    }

                    //
                    recValuesStr = dataReader.nextRec();

                    // Обеспечим не слишком огромные порции коммитов
                    countPortion++;
                    if (portionMax != 0 && countPortion >= portionMax) {
                        countPortion = 0;
                        //
                        db.commit();
                        db.startTran();
                        //
                        log.info("  table: " + readerTableName + ", " + count + ", commit/startTran");
                    }

                    //
                    count = count + 1;
                    if (count % 1000 == 0) {
                        long filePos = dataReader.getInputStream().getPos();
                        log.info("  table: " + readerTableName + ", recs: " + count + ", bytes: " + filePos + "/" + fileSize);
                    }
                }


                //
                log.info("  done: " + readerTableName + ", recs total: " + count);


                //
                readerTableName = dataReader.nextTable();
            }


            // Ворой проход - выполнение удаления отложенных
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
                        if (dbErrors.errorIs_ForeignKeyViolation(e)) {
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
