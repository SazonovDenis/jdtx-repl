package jdtx.repl.main.api;

import jandcode.dbm.db.Db;
import jandcode.dbm.db.DbQuery;
import jandcode.utils.UtCnv;
import jdtx.repl.main.api.struct.IJdxDbStruct;
import jdtx.repl.main.api.struct.IJdxFieldStruct;
import jdtx.repl.main.api.struct.IJdxTableStruct;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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


    public void readAuditData(String tableName, String tableFields, long ageFrom, long ageTo, JdxReplicaWriterXml dataWriter) throws Exception {
        IJdxTableStruct table = struct.getTable(tableName);

        //
        IRefDecoder decoder = new RefDecoder(db, wsId);

        //
        DbQuery rsTableLog = selectAuditData(tableName, tableFields, ageFrom, ageTo);
        try {
            if (!rsTableLog.eof()) {
                dataWriter.startTable(tableName);

                // Измененные данные помещаем в dataWriter
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
                    log.info("clearAudit: " + t.getName() + ", age: [" + ageFrom + ".." + ageTo + "], z_id: [" + fromId + ".." + toId + "], recs: " + (toId - fromId + 1));
                } else {
                    log.info("clearAudit: " + t.getName() + ", age: [" + ageFrom + ".." + ageTo + "], z_id: empty");
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

    protected DbQuery selectAuditData(String tableName, String tableFields, long ageFrom, long ageTo) throws Exception {
        //
        IJdxTableStruct tableFrom = struct.getTable(tableName);

        // Интервал id в таблице аудита, который покрывает возраст с ageFrom по ageTo
        long fromId = getAuditMaxIdByAge(tableFrom, ageFrom - 1) + 1;
        long toId = getAuditMaxIdByAge(tableFrom, ageTo);

        //
        if (toId >= fromId) {
            log.info("selectAudit: " + tableName + ", age: [" + ageFrom + ".." + ageTo + "], z_id: [" + fromId + ".." + toId + "], recs: " + (toId - fromId + 1));
        } else {
            log.info("selectAudit: " + tableName + ", age: [" + ageFrom + ".." + ageTo + "], z_id: empty");
        }

        // Аудит в указанном диапазоне возрастов: id >= fromId и id <= toId
        String sql = getSql(tableFrom, tableFields, fromId, toId);

        //
        return db.openSql(sql);
    }

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

    protected String getSql(IJdxTableStruct tableFrom, String tableFields, long fromId, long toId) {
        return "select " +
                JdxUtils.prefix + "opr_type, " + tableFields +
                " from " + JdxUtils.audit_table_prefix + tableFrom.getName() +
                " where " + JdxUtils.prefix + "id >= " + fromId + " and " + JdxUtils.prefix + "id <= " + toId +
                " order by " + JdxUtils.prefix + "id";
    }


}
