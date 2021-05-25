package jdtx.repl.main.api.audit;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_binder.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
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

    //
    protected static Log log = LogFactory.getLog("jdtx.AuditSelector");


    //
    public UtAuditSelector(Db db, IJdxDbStruct struct, long wsId) {
        this.db = db;
        this.struct = struct;
        this.wsId = wsId;
    }


    /**
     * Собрать аудит и подготовить реплику по правилам публикации publicationStorage для возраста age.
     */
    public IReplica createReplicaFromAudit(IPublicationRuleStorage publicationStorage, long age) throws Exception {
        log.info("createReplicaFromAudit, wsId: " + wsId + ", age: " + age);

        // Для выборки из аудита - узнаем интервалы id в таблицах аудита
        Map auditInfo = loadAutitIntervals(publicationStorage, age);

        //
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.IDE);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);
        replica.getInfo().setDtFrom((DateTime) auditInfo.get("z_opr_dttm_from"));
        replica.getInfo().setDtTo((DateTime) auditInfo.get("z_opr_dttm_to"));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Начинаем писать файл с данными
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument(); //<--- вот тут

        // Забираем аудит по порядку сортировки таблиц в struct
        for (IJdxTable structTable : struct.getTables()) {
            IPublicationRule publicationRule = publicationStorage.getPublicationRule(structTable.getName());
            if (publicationRule == null) {
                log.info("  skip table: " + structTable.getName() + ", not found in publicationStorage");
                continue;
            }
            //
            String publicationTableName = publicationRule.getTableName().toUpperCase();

            // Интервал id в таблице аудита, который покрывает возраст age
            Map autitInfoTable = (Map) auditInfo.get(publicationTableName);
            if (autitInfoTable != null) {
                long fromId = (long) autitInfoTable.get("z_id_from");
                long toId = (long) autitInfoTable.get("z_id_to");

                //
                log.info("createReplicaFromAudit: " + publicationTableName + ", age: " + age + ", z_id: [" + fromId + ".." + toId + "]");

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
        IJdxTable table = struct.getTable(tableName);
        IRefDecoder decoder = new RefDecoder(db, wsId);
        UtDataWriter utDataWriter = new UtDataWriter(table, tableFields, decoder, false);

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
                    // Пропуск аудита по изменению УЖЕ УДАЛЕННЫХ записей. Если такие команды оставить,
                    // то рабочая станция при ОТМЕНЕ УДАЛЕНИЯ не сможет выложить в очередь корректную реплику
                    // на "обратную вставку" - эта запись будет с пустыми полями как раз из-за того,
                    // что запрос аудита по изменению уже удаленных записей возвращает все поля null
                    // (что неудивительно, т.к. идет left join аудита и физической таблицы).
                    int z_skip = (int) dataTableAudit.getValue("z_skip");

                    // Не пропущенная запись
                    if (z_skip == 0) {
                        // record
                        dataWriter.appendRec();

                        // Тип операции
                        int oprType = (int) dataTableAudit.getValue(UtJdx.SQL_FIELD_OPR_TYPE);
                        dataWriter.setOprType(oprType);

                        // Тело записи (с перекодировкой ссылок)
                        utDataWriter.dataBinderRec_To_DataWriter_WithRefDecode(dataTableAudit, dataWriter);
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
                    log.info("clearAudit: " + t.getName() + ", age: [" + ageFrom + ".." + ageTo + "], z_id: [" + fromId + ".." + toId + "], audit recs: " + (toId - fromId + 1));
                } else {
                    log.debug("clearAudit: " + t.getName() + ", age: [" + ageFrom + ".." + ageTo + "], audit empty");
                }

                // изменения с указанным возрастом
                query = "delete from " + UtJdx.AUDIT_TABLE_PREFIX + t.getName() + " where " + UtJdx.PREFIX + "id >= :fromId and " + UtJdx.PREFIX + "id <= :toId";
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
        String query = "select " + UtJdx.PREFIX + "id as id from " + UtJdx.SYS_TABLE_PREFIX + "age where age=" + age + " and table_name='" + tableFrom.getName() + "'";
        return db.loadSql(query).getCurRec().getValueLong("id");
    }

    protected String getSql_full(IJdxTable tableFrom, String tableFields, long fromId, long toId) {
        return "select " +
                UtJdx.SQL_FIELD_OPR_TYPE + ", " + tableFields +
                " from " + UtJdx.AUDIT_TABLE_PREFIX + tableFrom.getName() +
                " where " + UtJdx.PREFIX + "id >= " + fromId + " and " + UtJdx.PREFIX + "id <= " + toId +
                " order by " + UtJdx.PREFIX + "id";
    }

    protected String getSql(IJdxTable tableFrom, String tableFields, long fromId, long toId) {
        String idFieldName = tableFrom.getPrimaryKey().get(0).getName();
        //
        String[] tableFromFields = tableFields.split(",");
        StringBuilder sb = new StringBuilder();
        for (String fieldName : tableFromFields) {
            if (fieldName.compareToIgnoreCase(idFieldName) == 0) {
                // без id из основной таблицы, id берем из таблицы аудита
                continue;
            }
            //
            if (sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(tableFrom.getName()).append(".").append(fieldName);
        }
        String tableFieldsAlias = sb.toString();

        //
        return "select " +
                UtJdx.SQL_FIELD_OPR_TYPE + ", " +
                UtJdx.PREFIX + "opr_dttm, " +
                UtJdx.AUDIT_TABLE_PREFIX + tableFrom.getName() + "." + idFieldName + ", " +
                "(case when " + UtJdx.SQL_FIELD_OPR_TYPE + " in (" + JdxOprType.OPR_INS + "," + JdxOprType.OPR_UPD + ") and " + tableFrom.getName() + "." + idFieldName + " is null then 1 else 0 end) z_skip, " +
                tableFieldsAlias +
                " from " + UtJdx.AUDIT_TABLE_PREFIX + tableFrom.getName() +
                " left join " + tableFrom.getName() + " on (" + UtJdx.AUDIT_TABLE_PREFIX + tableFrom.getName() + "." + idFieldName + " = " + tableFrom.getName() + "." + idFieldName + ")" +
                " where " + UtJdx.PREFIX + "id >= " + fromId + " and " + UtJdx.PREFIX + "id <= " + toId +
                " order by " + UtJdx.PREFIX + "id";
    }


    /**
     * Извлекает мин и макс z_id аудита для каждой таблицы,
     * а также общий период возникновения изменений в таблице.
     * Используем publicationStorage чтобы
     * - сэкономить на запросах - не делать запросов к таблицам, которые не в publicationStorage,
     * - а также чтобы избежать ошибок, делая запросы к таблицам, для которых не создан аудит (но которые есть в структуре).
     */
    Map loadAutitIntervals(IPublicationRuleStorage publicationStorage, long age) throws Exception {
        Map auditInfo = new HashMap<>();

        DateTime dtFrom = null;
        DateTime dtTo = null;

        for (IJdxTable structTable : struct.getTables()) {
            String tableName = structTable.getName();

            IPublicationRule publicationRule = publicationStorage.getPublicationRule(tableName);
            if (publicationRule == null) {
                log.info("  skip table: " + tableName + ", not found in publicationStorage");
                continue;
            }


            String sql = "select\n" +
                    "  --z_z_age_last.table_name,\n" +
                    "  z_z_age_prior.age age_prior,\n" +
                    "  z_z_age_last.age age_last,\n" +
                    "  max(z_prior.z_id) z_id_from,\n" +
                    "  z_last.z_id z_id_to,\n" +
                    "  max(z_prior.z_opr_dttm) opr_dttm_from,\n" +
                    "  z_last.z_opr_dttm opr_dttm_to,\n" +
                    "  --z_z_age_last.dt,\n" +
                    "  --z_z_age_prior.dt dt_prior,\n" +
                    "  1 as x\n" +
                    "from\n" +
                    "  z_z_age z_z_age_last\n" +
                    "  join z_z_age z_z_age_prior on (z_z_age_prior.table_name = '" + tableName + "' and z_z_age_prior.age = " + (age - 1) + ")\n" +
                    "  left join z_" + tableName + " z_prior on (z_z_age_prior.z_id >= z_prior.z_id)\n" +
                    "  left join z_" + tableName + " z_last on (z_z_age_last.z_id = z_last.z_id)\n" +
                    "where\n" +
                    "  z_z_age_last.age = " + age + " and\n" +
                    "  z_z_age_last.table_name = '" + tableName + "' and\n" +
                    "  1=1\n" +
                    "group by\n" +
                    "  1,2,4,6";

            //
            DataStore st = db.loadSql(sql);

            // Аудит для для таблицы накопился?
            if (st.size() == 0) {
                continue;
            }

            //
            DataRecord rec = st.get(0);

            // Аудит для этого возраста пуст?
            long z_id_from = rec.getValueLong("z_id_from") + 1;
            long z_id_to = rec.getValueLong("z_id_to");
            if (z_id_from > z_id_to) {
                continue;
            }

            // Собираем мин и макс даты возникновения аудита
            if (dtFrom == null || dtFrom.compareTo(rec.getValueDateTime("opr_dttm_from")) > 0) {
                dtFrom = rec.getValueDateTime("opr_dttm_from");
            }
            if (dtTo == null || dtTo.compareTo(rec.getValueDateTime("opr_dttm_to")) < 0) {
                dtTo = rec.getValueDateTime("opr_dttm_to");
            }

            // Набираем выходные данные об аудите
            Map auditInfoTable = new HashMap<>();
            auditInfoTable.put("z_id_from", z_id_from);
            auditInfoTable.put("z_id_to", z_id_to);
            auditInfo.put(tableName, auditInfoTable);
        }

        //
        auditInfo.put("z_opr_dttm_from", dtFrom);
        auditInfo.put("z_opr_dttm_to", dtTo);

        //
        return auditInfo;
    }
}
