package jdtx.repl.main.api.audit;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_binder.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.ref_manager.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.joda.time.*;

import java.util.*;

/**
 * Извлекатель данных по аудиту
 */
public class UtAuditSelector {

    private Db db;
    private IJdxDbStruct struct;
    long wsId;
    IDbNamesManager dbNamesManager;

    //
    protected static Log log = LogFactory.getLog("jdtx.AuditSelector");


    //
    public UtAuditSelector(Db db, IJdxDbStruct struct, long wsId) {
        this.db = db;
        this.struct = struct;
        this.wsId = wsId;
        this.dbNamesManager = db.getApp().service(DbToolsService.class).getDbNamesManager(db);
    }


    /**
     * Собрать аудит и подготовить реплику по правилам публикации publicationRules для возраста age.
     */
    public IReplica createReplicaFromAudit(IPublicationRuleStorage publicationRules, long age) throws Exception {
        log.info("createReplicaFromAudit, wsId: " + wsId + ", age: " + age);

        // Для выборки из аудита - узнаем интервалы id в таблицах аудита
        Map auditInfo = loadAutitIntervals(publicationRules, age);

        //
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.IDE);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);
        replica.getInfo().setDtFrom((DateTime) auditInfo.get(UtJdx.AUDIT_FIELD_PREFIX + "opr_dttm_from"));
        replica.getInfo().setDtTo((DateTime) auditInfo.get(UtJdx.AUDIT_FIELD_PREFIX + "opr_dttm_to"));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Начинаем писать файл с данными
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDat();

        // Забираем аудит по порядку сортировки таблиц в struct
        for (IJdxTable structTable : struct.getTables()) {
            IPublicationRule publicationRule = publicationRules.getPublicationRule(structTable.getName());
            if (publicationRule == null) {
                log.info("  skip table: " + structTable.getName() + ", not found in publicationRules");
                continue;
            }
            //
            String publicationTableName = publicationRule.getTableName().toUpperCase();

            // Интервал id в таблице аудита, который покрывает возраст age
            Map autitInfoTable = (Map) auditInfo.get(publicationTableName);
            if (autitInfoTable != null) {
                long fromId = (long) autitInfoTable.get(UtJdx.AUDIT_FIELD_PREFIX + "id_from");
                long toId = (long) autitInfoTable.get(UtJdx.AUDIT_FIELD_PREFIX + "id_to");

                //
                log.info("createReplicaFromAudit: " + publicationTableName + ", age: " + age + ", " + UtJdx.AUDIT_FIELD_PREFIX + "id: [" + fromId + ".." + toId + "]");

                //
                String publicationFields = UtJdx.fieldsToString(publicationRule.getFields());
                readAuditData_ByInterval(publicationTableName, publicationFields, fromId, toId, xmlWriter);
            }
        }

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    void readAuditData_ByInterval(String tableName, String tableFields, long fromId, long toId, JdxReplicaWriterXml dataWriter) throws Exception {
        // Serializer
        RefManagerService refManagerService = db.getApp().service(RefManagerService.class);
        IJdxDataSerializer dataSerializer = refManagerService.getJdxDataSerializer();
        // Таблица и поля в Serializer-е
        IJdxTable table = struct.getTable(tableName);
        dataSerializer.setTable(table, tableFields);

        // DbQuery, содержащий аудит в указанном диапазоне: id >= fromId и id <= toId
        IJdxTable tableFrom = struct.getTable(tableName);
        String sql = getSql(tableFrom, tableFields, fromId, toId);
        DbQuery query = db.openSql(sql);
        IJdxDataBinder dataTableAudit = new JdxDataBinder_DbQuery(query);

        //
        try {
            if (!dataTableAudit.eof()) {
                // Журнал аудита для таблицы (измененные записи) кладем в dataWriter
                dataWriter.startTable(tableName);

                //
                long n = 0;
                while (!dataTableAudit.eof()) {
                    Map<String, Object> values = dataTableAudit.getValues();

                    // Пропуск аудита по изменению УЖЕ УДАЛЕННЫХ записей. Если такие команды оставить,
                    // то рабочая станция при ОТМЕНЕ УДАЛЕНИЯ не сможет выложить в очередь корректную реплику
                    // на "обратную вставку" - эта запись будет с пустыми полями как раз из-за того,
                    // что запрос аудита по изменению уже удаленных записей возвращает все поля null
                    // (что неудивительно, т.к. идет left join аудита и физической таблицы).
                    int z_skip = (int) values.get(UtJdx.AUDIT_FIELD_PREFIX + "skip");

                    // Не пропущенная запись
                    if (z_skip == 0) {
                        // record
                        dataWriter.appendRec();

                        // Тип операции
                        JdxOprType oprType = JdxOprType.valueOfStr(String.valueOf(values.get(UtJdx.SQL_FIELD_OPR_TYPE)));
                        dataWriter.writeOprType(oprType);

                        // Тело записи (с перекодировкой ссылок)
                        Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
                        UtXml.recToWriter(valuesStr, tableFields, dataWriter);
                    }

                    //
                    dataTableAudit.next();

                    //
                    n++;
                    if (n % 1000 == 0) {
                        log.info("readData: " + tableName + ", " + n);
                    }
                }

                //
                log.info("readData: " + tableName + ", total: " + n);
            }

            //
            dataWriter.flush();
        } finally {
            dataTableAudit.close();
        }
    }

    // todo: все-таки задействовать когда нибудь, когда все устаканится. Дергать по команде с сервера, когда все реплики получены и обработаны
    public void __clearAuditData(long ageFrom, long ageTo) throws Exception {
        String query;
        db.startTran();
        try {
            // удаляем журнал измений во всех таблицах
            for (IJdxTable t : struct.getTables()) {
                // Интервал id в таблице аудита, который покрывает возраст с ageFrom по ageTo
                long fromId = getAuditMaxIdByAge(t, ageFrom - 1) + 1;
                long toId = getAuditMaxIdByAge(t, ageTo);

                //
                if (toId >= fromId) {
                    log.info("clearAudit: " + t.getName() + ", age: [" + ageFrom + ".." + ageTo + "], " + UtJdx.AUDIT_FIELD_PREFIX + "id: [" + fromId + ".." + toId + "], audit recs: " + (toId - fromId + 1));
                } else {
                    log.debug("clearAudit: " + t.getName() + ", age: [" + ageFrom + ".." + ageTo + "], audit empty");
                }

                // изменения с указанным возрастом
                String auditTableName = dbNamesManager.getShortName(t.getName(), UtJdx.AUDIT_TABLE_PREFIX);
                query = "delete from " + auditTableName + " where " + UtJdx.AUDIT_FIELD_PREFIX + "id >= :fromId and " + UtJdx.AUDIT_FIELD_PREFIX + "id <= :toId";
                db.execSql(query, UtCnv.toMap("fromId", fromId, "toId", toId));
            }
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }
    }


    /**
     * Возвращает, на каком ID таблицы аудита закончилась реплика с возрастом age
     *
     * @param tableFrom для какой таблицы
     * @param age       возраст БД
     * @return id таблицы аудита, соответствующая возрасту age
     */
    protected long getAuditMaxIdByAge(IJdxTable tableFrom, long age) throws Exception {
        String query = "select " + UtJdx.AUDIT_FIELD_PREFIX + "id as id from " + UtJdx.SYS_TABLE_PREFIX + "age where age=" + age + " and table_name='" + tableFrom.getName() + "'";
        return db.loadSql(query).getCurRec().getValueLong("id");
    }

    protected String getSql_full(IJdxTable tableFrom, String tableFields, long fromId, long toId) {
        String tableName = tableFrom.getName();
        String auditTableName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TABLE_PREFIX);
        return "select " +
                UtJdx.SQL_FIELD_OPR_TYPE + ", " + tableFields +
                " from " + auditTableName +
                " where " + UtJdx.AUDIT_FIELD_PREFIX + "id >= " + fromId + " and " + UtJdx.AUDIT_FIELD_PREFIX + "id <= " + toId +
                " order by " + UtJdx.AUDIT_FIELD_PREFIX + "id";
    }

    protected String getSql(IJdxTable tableFrom, String tableFields, long fromId, long toId) {
        String pkFieldName = tableFrom.getPrimaryKey().get(0).getName();
        String tableName = tableFrom.getName();
        //
        String[] tableFromFields = tableFields.split(",");
        StringBuilder sb = new StringBuilder();
        for (String fieldName : tableFromFields) {
            if (fieldName.compareToIgnoreCase(pkFieldName) == 0) {
                // без id из основной таблицы, id берем из таблицы аудита
                continue;
            }
            //
            if (sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(tableName).append(".").append(fieldName);
        }
        String tableFieldsAlias = sb.toString();

        //
        String auditTableName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TABLE_PREFIX);
        return "select " +
                UtJdx.SQL_FIELD_OPR_TYPE + ", \n" +
                UtJdx.AUDIT_FIELD_PREFIX + "opr_dttm, \n" +
                auditTableName + "." + pkFieldName + ", \n" +
                "(case when " + UtJdx.SQL_FIELD_OPR_TYPE + " in (" + JdxOprType.INS.getValue() + "," + JdxOprType.UPD.getValue() + ") and " + tableName + "." + pkFieldName + " is null then 1 else 0 end) " + UtJdx.AUDIT_FIELD_PREFIX + "skip, \n" +
                tableFieldsAlias + "\n" +
                " from " + auditTableName + "\n" +
                " left join " + tableName + " on (" + auditTableName + "." + pkFieldName + " = " + tableName + "." + pkFieldName + ") \n" +
                " where " + UtJdx.AUDIT_FIELD_PREFIX + "id >= " + fromId + " and " + UtJdx.AUDIT_FIELD_PREFIX + "id <= " + toId + "\n" +
                " order by " + UtJdx.AUDIT_FIELD_PREFIX + "id";
    }


    /**
     * Извлекает мин и макс zz_id аудита для каждой таблицы,
     * а также общий период возникновения изменений в таблице.
     * Используем publicationStorage чтобы
     * - сэкономить на запросах - не делать запросов к таблицам, которые не в publicationStorage,
     * - а также чтобы избежать ошибок, делая запросы к таблицам, для которых не создан аудит (но которые есть в структуре).
     */
    Map loadAutitIntervals(IPublicationRuleStorage publicationStorage, long age) throws Exception {
        Map auditInfo = new HashMap<>();

        //
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        //
        long age_prior = age - 1;
        Map<String, Long> maxIdsFixed_From = new HashMap<>();
        DateTime dtFrom = auditAgeManager.loadMaxIdsFixed(age_prior, maxIdsFixed_From);
        //
        Map<String, Long> maxIdsFixed_To = new HashMap<>();
        DateTime dtTo = auditAgeManager.loadMaxIdsFixed(age, maxIdsFixed_To);

        //
        auditInfo.put(UtJdx.AUDIT_FIELD_PREFIX + "opr_dttm_from", dtFrom);
        auditInfo.put(UtJdx.AUDIT_FIELD_PREFIX + "opr_dttm_to", dtTo);

        // Собираем данные об аудите таблиц
        for (IJdxTable structTable : struct.getTables()) {
            String tableName = structTable.getName();

            // Нет правила публикации - не анализируем аудит
            IPublicationRule publicationRule = publicationStorage.getPublicationRule(tableName);
            if (publicationRule == null) {
                log.info("  skip table: " + tableName + ", not found in publicationStorage");
                continue;
            }

            //
            long z_id_age_prior = UtJdxData.longValueOf(maxIdsFixed_From.get(tableName), 0L);
            long z_id_age = UtJdxData.longValueOf(maxIdsFixed_To.get(tableName), 0L);

            // Аудит для таблицы для перехода к возрасту age пуст?
            if (z_id_age_prior == z_id_age) {
                continue;
            }

            // Аудит для таблицы НЕ пуст, набираем выходные данные об аудите измененной таблицы
            Map auditInfoTable = new HashMap<>();
            auditInfoTable.put(UtJdx.AUDIT_FIELD_PREFIX + "id_from", z_id_age_prior + 1);
            auditInfoTable.put(UtJdx.AUDIT_FIELD_PREFIX + "id_to", z_id_age);
            auditInfo.put(tableName, auditInfoTable);
        }


        //
        return auditInfo;
    }
}
