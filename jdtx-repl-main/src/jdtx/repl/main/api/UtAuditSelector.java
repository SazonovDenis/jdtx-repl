package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.joda.time.*;

import java.util.*;

public class UtAuditSelector {

    private Db db;
    private IJdxDbStruct struct;
    long wsId;

    //
    protected static Log log = LogFactory.getLog("jdtx");


    //
    public UtAuditSelector(Db db, IJdxDbStruct struct, long wsId) {
        this.db = db;
        this.struct = struct;
        this.wsId = wsId;
    }


/*
    public void readAuditData_old(String tableName, String tableFields, long ageFrom, long ageTo, JdxReplicaWriterXml dataWriter) throws Exception {
        IJdxTableStruct table = struct.getTable(tableName);

        //
        IRefDecoder decoder = new RefDecoder(db, wsId);


        //
        DbQuery rsTableLog = selectAuditData_old(tableName, tableFields, ageFrom, ageTo);
        try {
            if (!rsTableLog.eof()) {
                // table
                dataWriter.startTable(tableName);

                // Журнал аудита (измененные записи) кладем в dataWriter
                long n = 0;
                while (!rsTableLog.eof()) {
                    dataWriter.appendRec();

                    // Тип операции
                    dataWriter.setOprType(rsTableLog.getValueInt(JdxUtils.prefix + "opr_type"));

                    // Тело записи
                    String[] tableFromFields = tableFields.split(",");
                    for (String fieldName : tableFromFields) {
                        Object fieldValue = rsTableLog.getValue(fieldName);
                        IJdxFieldStruct field = table.getField(fieldName);
                        IJdxTableStruct refTable = field.getRefTable();
                        if (field.isPrimaryKey() || refTable != null) {
                            // Ссылка
                            String refTableName;
                            if (field.isPrimaryKey()) {
                                refTableName = table.getName();
                            } else {
                                refTableName = refTable.getName();
                            }
                            // Перекодировка ссылки
                            JdxRef ref = decoder.get_ref(refTableName, Long.valueOf(String.valueOf(fieldValue)));
                            dataWriter.setRecValue(fieldName, ref.toString());
                        } else {
                            dataWriter.setRecValue(fieldName, fieldValue);
                        }


                    }

                    //
                    rsTableLog.next();

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
            rsTableLog.close();
        }
    }
*/

    protected void readAuditData_ById(String tableName, String tableFields, long fromId, long toId, JdxReplicaWriterXml dataWriter) throws Exception {
        IJdxTableStruct table = struct.getTable(tableName);

        // decoder
        IRefDecoder decoder = new RefDecoder(db, wsId);

        // DbQuery, содержащий аудит в указанном диапазоне: id >= fromId и id <= toId
        IJdxTableStruct tableFrom = struct.getTable(tableName);
        String sql = getSql(tableFrom, tableFields, fromId, toId);
        DbQuery rsTableLog = db.openSql(sql);

        //
        try {
            if (!rsTableLog.eof()) {
                // Журнал аудита для таблицы (измененные записи) кладем в dataWriter
                dataWriter.startTable(tableName);

                //
                long n = 0;
                while (!rsTableLog.eof()) {
                    // record
                    dataWriter.appendRec();

                    // Тип операции
                    dataWriter.setOprType(rsTableLog.getValueInt(JdxUtils.prefix + "opr_type"));

                    // Тело записи
                    String[] tableFromFields = tableFields.split(",");
                    for (String fieldName : tableFromFields) {
                        Object fieldValue = rsTableLog.getValue(fieldName);
                        IJdxFieldStruct field = table.getField(fieldName);
                        IJdxTableStruct refTable = field.getRefTable();
                        if (field.isPrimaryKey() || refTable != null) {
                            // Ссылка
                            String refTableName;
                            if (field.isPrimaryKey()) {
                                refTableName = table.getName();
                            } else {
                                refTableName = refTable.getName();
                            }
                            // Перекодировка ссылки
                            JdxRef ref = decoder.get_ref(refTableName, Long.valueOf(String.valueOf(fieldValue)));
                            dataWriter.setRecValue(fieldName, ref.toString());
                        } else {
                            dataWriter.setRecValue(fieldName, fieldValue);
                        }


                    }

                    //
                    rsTableLog.next();

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
            rsTableLog.close();
        }
    }

    public void __clearAuditData(long ageFrom, long ageTo) throws Exception {
        String query;
        db.startTran();
        try {
            // удаляем журнал измений во всех таблицах
            for (IJdxTableStruct t : struct.getTables()) {
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
                query = "delete from " + JdxUtils.audit_table_prefix + t.getName() + " where " + JdxUtils.prefix + "id >= :fromId and " + JdxUtils.prefix + "id <= :toId";
                db.execSql(query, UtCnv.toMap("fromId", fromId, "toId", toId));
            }
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }
    }

/*
    protected DbQuery selectAuditData_old(String tableName, String tableFields, long ageFrom, long ageTo) throws Exception {
        //
        IJdxTableStruct tableFrom = struct.getTable(tableName);

        // Интервал id в таблице аудита, который покрывает возраст с ageFrom по ageTo
        long fromId = getAuditMaxIdByAge(tableFrom, ageFrom - 1) + 1;
        long toId = getAuditMaxIdByAge(tableFrom, ageTo);

        //
        if (toId >= fromId) {
            log.info("selectAudit: " + tableName + ", age: [" + ageFrom + ".." + ageTo + "], z_id: [" + fromId + ".." + toId + "], audit recs: " + (toId - fromId + 1));
        } else {
            //log.debug("selectAudit: " + tableName + ", age: [" + ageFrom + ".." + ageTo + "], audit empty");
        }

        // Аудит в указанном диапазоне: id >= fromId и id <= toId
        String sql = getSql(tableFrom, tableFields, fromId, toId);

        //
        return db.openSql(sql);
    }
*/

    /**
     * Возвращает, на каком ID таблицы аудита закончилась реплика с возрастом age
     *
     * @param tableFrom для какой таблицы
     * @param age       возраст БД
     * @return id таблицы аудита, соответствующая возрасту age
     */
    protected long getAuditMaxIdByAge(IJdxTableStruct tableFrom, long age) throws Exception {
        String query = "select " + JdxUtils.prefix + "id as id from " + JdxUtils.sys_table_prefix + "age where age=" + age + " and table_name='" + tableFrom.getName() + "'";
        return db.loadSql(query).getCurRec().getValueLong("id");
    }

    protected String getSql_full(IJdxTableStruct tableFrom, String tableFields, long fromId, long toId) {
        return "select " +
                JdxUtils.prefix + "opr_type, " + tableFields +
                " from " + JdxUtils.audit_table_prefix + tableFrom.getName() +
                " where " + JdxUtils.prefix + "id >= " + fromId + " and " + JdxUtils.prefix + "id <= " + toId +
                " order by " + JdxUtils.prefix + "id";
    }

    protected String getSql(IJdxTableStruct tableFrom, String tableFields, long fromId, long toId) {
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
                JdxUtils.prefix + "opr_type, " +
                JdxUtils.prefix + "opr_dttm, " +
                JdxUtils.audit_table_prefix + tableFrom.getName() + "." + idFieldName + ", " +
                tableFieldsAlias +
                " from " + JdxUtils.audit_table_prefix + tableFrom.getName() +
                " left join " + tableFrom.getName() + " on (" + JdxUtils.audit_table_prefix + tableFrom.getName() + "." + idFieldName + " = " + tableFrom.getName() + "." + idFieldName + ")" +
                " where " + JdxUtils.prefix + "id >= " + fromId + " and " + JdxUtils.prefix + "id <= " + toId +
                " order by " + JdxUtils.prefix + "id";
    }


    /**
     * Извлекает мин и макс z_id аудита для каждой таблицы,
     * а также общий период возникновения изменений в таблице.
     */
    public Map loadAutitIntervals(IPublication publication, long age) throws Exception {
        Map auditInfo = new HashMap<>();

        DateTime dtFrom = null;
        DateTime dtTo = null;

        for (IJdxTableStruct table : publication.getData().getTables()) {
/*
            String stuctTableName = table.getName();
            boolean foundInPublication = false;
            for (int i = 0; i < publicationData.size(); i++) {
                JSONObject publicationTable = (JSONObject) publicationData.get(i);
                String publicationTableName = (String) publicationTable.get("table");
                if (stuctTableName.compareToIgnoreCase(publicationTableName) == 0) {
                    foundInPublication = true;
                    break;
                }
            }

            //
            if (foundInPublication) {
*/
            //String publicationFields = Publication.prepareFiledsString(table, (String) publicationTable.get("fields"));
            //utrr.readAuditData(stuctTableName, publicationFields, age - 1, age, writerXml);
            //
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
                    "  join z_z_age z_z_age_prior on (z_z_age_prior.table_name = '" + table.getName() + "' and z_z_age_prior.age = " + (age - 1) + ")\n" +
                    "  left join z_" + table.getName() + " z_prior on (z_z_age_prior.z_id >= z_prior.z_id)\n" +
                    "  left join z_" + table.getName() + " z_last on (z_z_age_last.z_id = z_last.z_id)\n" +
                    "where\n" +
                    "  z_z_age_last.age = " + age + " and\n" +
                    "  z_z_age_last.table_name = '" + table.getName() + "' and\n" +
                    "  1=1\n" +
                    "group by\n" +
                    "  1,2,4,6";

            String sql_old = "select\n" +
                    "  z_z_age_last.table_name,\n" +
                    "  z_z_age_prior.age age_prior,\n" +
                    "  z_z_age_last.age,\n" +
                    "  z_data_prior.z_id z_id_from,\n" +
                    "  z_data_last.z_id z_id_to,\n" +
                    "  z_data_prior.z_opr_dttm opr_dttm_from,\n" +
                    "  z_data_last.z_opr_dttm opr_dttm_to,\n" +
                    "  --z_z_age_last.dt,\n" +
                    "  --z_z_age_prior.dt dt_prior,\n" +
                    "  1 as x\n" +
                    "from\n" +
                    "  z_z_age z_z_age_prior, z_z_age z_z_age_last, z_" + table.getName() + " z_data_prior, z_" + table.getName() + " z_data_last\n" +
                    "where\n" +
                    "  z_z_age_last.table_name = z_z_age_prior.table_name and\n" +
                    "  z_z_age_last.age = z_z_age_prior.age + 1 and\n" +
                    "  --\n" +
                    "  z_z_age_prior.z_id + 1 = z_data_prior.z_id and\n" +
                    "  z_z_age_last.z_id = z_data_last.z_id and\n" +
                    "  --\n" +
                    "  z_z_age_last.age = " + age + " and\n" +
                    "  z_z_age_last.table_name = '" + table.getName() + "' and\n" +
                    "  1=1\n" +
                    "order by\n" +
                    "  z_z_age_last.table_name, z_z_age_last.age";

            //
            DataStore st = db.loadSql(sql);

            // Аудит для для таблицы существует?
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
            auditInfo.put(table.getName(), auditInfoTable);
        }

        //
        auditInfo.put("z_opr_dttm_from", dtFrom);
        auditInfo.put("z_opr_dttm_to", dtTo);

        //
        return auditInfo;
    }
}
