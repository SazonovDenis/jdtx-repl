package jdtx.repl.main.api.audit;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.decoder.*;
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

    public void applyReplica(IReplica replica, IPublicationStorage publicationIn, boolean forceApply_ignorePublicationRules, long commitPortionMax) throws Exception {
        //
        InputStream inputStream = null;
        try {
            // Распакуем XML-файл из Zip-архива
            inputStream = JdxReplicaReaderXml.createInputStreamData(replica);

            //
            JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

            //
            try {
                applyReplicaReader(replicaReader, publicationIn, forceApply_ignorePublicationRules, commitPortionMax);
            } catch (Exception e) {
                if (e instanceof JdxForeignKeyViolationException) {
                    File replicaRepairFile = jdxReplWs.handleFailedInsertUpdateRef((JdxForeignKeyViolationException) e);
                    if (false) {
                        log.error("==================================");
                        log.error("==================================");
                        log.error("==================================");
                        log.error("Восстанавливаем записи из реплики: " + replicaRepairFile.getAbsolutePath());
                        jdxReplWs.useReplicaFile(replicaRepairFile);
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
    public void applyReplicaReader(JdxReplicaReaderXml dataReader, IPublicationStorage publicationRules, boolean forceApply_ignorePublicationRules, long portionMax) throws Exception {
        log.info("applyReplica, self.WsId: " + wsId + ", replica.WsId: " + dataReader.getWsId() + ", replica.age: " + dataReader.getAge());

        //
        List<IJdxTable> tables = struct.getTables();
        int tIdx = 0;

        //
        JdxDbUtils dbu = new JdxDbUtils(db, struct);

        //
        AuditDbTriggersManager triggersManager = new AuditDbTriggersManager(db);

        //
        IRefDecoder decoder = new RefDecoder(db, wsId);

        //
        db.startTran();
        try {
            // На время применения аудита нужно выключать триггера
            triggersManager.setTriggersOff();


            // Тут копим задания на DELETE для всей реплики, их выполняем вторым проходом.
            // Если пытаться выполнить реплики все сразу, то возможна нежелательная ситуация:
            // Например, при слиянии значений в справочнике CommentTip появятся реплики:
            // CommentTip:
            //  - вставить запись 5
            //  - удалить записи 1, 2, 3, 4
            // CommentText:
            //  - заменить ссылку CommentTip с 1 на 5
            //  - заменить ссылку CommentTip с 2 на 5
            //  - заменить ссылку CommentTip с 3 на 5
            //  - удалить запись со ссылкой CommentTip раной 4
            //
            // Этот поток реплик, хотя является в конечном итоге правильным, тем не менее, не может быть обработан без ошибок,
            // т.к из за ссылочной целостности его нужно выполнять в такой последовательности:
            // CommentTip:
            //  - вставить запись 4
            // CommentText:
            //  - заменить ссылку CommentTip с 1 на 5
            //  - заменить ссылку CommentTip с 2 на 5
            //  - заменить ссылку CommentTip с 3 на 5
            //  - удалить запись со ссылкой CommentTip раной 4
            // CommentTip:
            //  - удалить записи 1, 2, 3, 4
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
                    throw new XError("table [" + readerTableName + "] found in replica data, but not found in dbstruct");
                }
                //
                tIdx = n;
                IJdxTable table = tables.get(n);
                String idFieldName = table.getPrimaryKey().get(0).getName();
                String tableName = table.getName();

                // Поиск таблицы и ее полей в публикации (поля берем именно из правил публикаций)
                IPublicationRule publicationRuleTable;
                if (forceApply_ignorePublicationRules) {
                    // Таблицу и ее поля берем из актуальной структуры БД
                    publicationRuleTable = new PublicationRule(struct.getTable(readerTableName));
                    log.info("  force apply table: " + readerTableName + ", ignore publication rules");
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
                }

                // Тут копим задания на DELETE для таблицы, их выполняем вторым проходом.
                Collection<Long> delayedDeleteList = delayedDeleteTask.get(tableName);
                if (delayedDeleteList == null) {
                    delayedDeleteList = new ArrayList<>();
                    delayedDeleteTask.put(tableName, delayedDeleteList);
                }

                // Перебираем записи
                countPortion = 0;
                if (readerTableName.compareToIgnoreCase(readerTableNamePrior) == 0) {
                    log.info("next portion: " + readerTableName);
                } else {
                    count = 0;
                }
                readerTableNamePrior = readerTableName;

                Map recValues = dataReader.nextRec();
                while (recValues != null) {
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

                    // Подготовка recParams - значений полей для записи в БД
                    Map recParams = new HashMap();
                    for (IJdxField publicationField : publicationRuleTable.getFields()) {
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
                            DateTime value = null;
                            if (valueStr != null && valueStr.length() != 0) {
                                value = new DateTime(valueStr);
                            }
                            recParams.put(publicationFieldName, value);
                            continue;
                        }

                        // Поле - ссылка?
                        IJdxTable refTable = field.getRefTable();
                        if (field.isPrimaryKey() || refTable != null) {
                            // Ссылка
                            String refTableName;
                            if (field.isPrimaryKey()) {
                                refTableName = tableName;
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
                    String publicationFields = UtJdx.fieldsToString(publicationRuleTable.getFields());
                    // Очень важно взять поля для обновления (publicationFields) именно из правил публикации,
                    // а не все что есть в физической  таблице, т.к. именно по этим правилам готовилась реплика на сервере,
                    // при этом может импользоваться НЕ ПОЛНЫЙ набор полей. (Неполный набр полей используется, например,
                    // если на филиалы НЕ отправляются данные из справочников, на которые ссылается рассматриваемая таблица,
                    // например: примечания, сделанные пользователем, примечания отправляем, а ССЫЛКИ на пользователей придется пропустить),
                    // Из-за этого пропуска полей, при получении на рабочей станции СВОЕЙ реплики и попытке обновить ВСЕ поля,
                    // пропущенные поля станут null. На ДРУГИХ филиалах это не страшно, а на НАШЕЙ - данные затрутся.
                    int oprType = UtJdx.intValueOf(recValues.get(UtJdx.XML_FIELD_OPR_TYPE));
                    if (oprType == JdxOprType.OPR_INS) {
                        try {
                            insertOrUpdate(dbu, tableName, recParams, publicationFields);
                        } catch (Exception e) {
                            if (UtJdx.errorIs_ForeignKeyViolation(e)) {
                                log.error(e.getMessage());
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
                            dbu.updateRec(tableName, recParams, publicationFields, null);
                        } catch (Exception e) {
                            if (UtJdx.errorIs_ForeignKeyViolation(e)) {
                                log.error(e.getMessage());
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
                        // Отложим удаление на второй проход
                        delayedDeleteList.add((Long) recParams.get(idFieldName));
                    }

                    //
                    recValues = dataReader.nextRec();

                    //
                    count++;
                    if (count % 200 == 0) {
                        log.info("  table: " + readerTableName + ", " + count);
                    }
                }


                //
                log.info("  done: " + readerTableName + ", total: " + count);


                //
                readerTableName = dataReader.nextTable();
            }


            // Ворой проход - выполнение удаления отложенных
            //log.info("applyReplica, delayed delete");
            for (int i = tables.size() - 1; i >= 0; i--) {
                IJdxTable table = tables.get(i);
                String tableName = table.getName();

                //
                Collection<Long> delayedDeleteList = delayedDeleteTask.get(tableName);
                if (delayedDeleteList == null) {
                    continue;
                }
                if (delayedDeleteList.size() == 0) {
                    continue;
                }

                //
                List<Long> failedDeleteList = new ArrayList<>();
                count = 0;
                for (Long recId : delayedDeleteList) {
                    try {
                        dbu.deleteRec(tableName, recId);
                        count = count + 1;
                    } catch (Exception e) {
                        if (UtJdx.errorIs_ForeignKeyViolation(e)) {
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
                    count++;
                    if (count % 200 == 0) {
                        log.info("  table delete: " + tableName + ", " + count);
                    }

                }

                //
                log.info("  done delete: " + tableName + ", total: " + count);

                // Обратка от удалений, которые не удалось выполнить - создаем реплики на вставку (выдадим в исходящую очередь наш вариант удаляемой записи),
                // чтобы те, кто уже удалил - раскаялись и вернули все назад, по данныим из наших реплик.
                // todo: этот метод В КОНТЕКТСЕ ТРАНЗАКЦИИ возится с какими то файлами и проч... - нежелательно
                // todo: комит тут внутри, а контекст с этим методом createTableReplicaByIdList() - снаружи
                if (failedDeleteList.size() != 0) {
                    log.info("  failed delete: " + tableName + ", count: " + failedDeleteList.size());
                    //
                    jdxReplWs.createTableReplicaByIdList(tableName, failedDeleteList);
                    //
                    log.info("  failed delete: " + tableName + ", done");
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

    private void insertOrUpdate(JdxDbUtils dbu, String tableName, Map recParams, String publicationFields) throws Exception {
        try {
            dbu.insertRec(tableName, recParams, publicationFields, null);
        } catch (Exception e) {
            if (UtJdx.errorIs_PrimaryKeyError(e)) {
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