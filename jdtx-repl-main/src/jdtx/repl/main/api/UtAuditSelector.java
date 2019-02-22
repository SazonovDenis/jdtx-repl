package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public class UtAuditSelector {

    private Db db;
    private IJdxDbStruct struct;

    protected static Log log = LogFactory.getLog("jdtx");

    public UtAuditSelector(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }


    public void readAuditData(String tableName, String tableFields, long ageFrom, long ageTo, JdxReplicaWriterXml dataWriter) throws Exception {
        //
        DbQuery rsTableLog = selectAuditData(tableName, tableFields, ageFrom, ageTo);
        try {
            dataWriter.startTable(tableName);

            // ���������� ������ �������� � dataWriter
            while (!rsTableLog.eof()) {
                dataWriter.append();
                // ��� ��������
                dataWriter.setOprType(rsTableLog.getValueInt(JdxUtils.prefix + "opr_type"));
                // ���� ������
                String[] tableFromFields = tableFields.split(",");
                for (String field : tableFromFields) {
                    dataWriter.setRecValue(field, rsTableLog.getValue(field));
                }
                //
                rsTableLog.next();
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
            // ������� ������ ������� �� ���� ��������
            for (IJdxTableStruct t : struct.getTables()) {
                // �������� id � ������� ������, ������� ��������� ������� � ageFrom �� ageTo
                long fromId = getAuditMaxIdByAge(t, ageFrom - 1) + 1;
                long toId = getAuditMaxIdByAge(t, ageTo);

                //
                if (toId >= fromId) {
                    log.info("clearAudit: " + t.getName() + ", age: [" + ageFrom + ".." + ageTo + "], z_id: [" + fromId + ".." + toId + "], count: " + (toId - fromId + 1));
                } else {
                    log.info("clearAudit: " + t.getName() + ", age: [" + ageFrom + ".." + ageTo + "], z_id: empty");
                }

                // ��������� � ��������� ���������
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

        // �������� id � ������� ������, ������� ��������� ������� � ageFrom �� ageTo
        long fromId = getAuditMaxIdByAge(tableFrom, ageFrom - 1) + 1;
        long toId = getAuditMaxIdByAge(tableFrom, ageTo);

        //
        if (toId >= fromId) {
            log.info("selectAudit: " + tableName + ", age: [" + ageFrom + ".." + ageTo + "], z_id: [" + fromId + ".." + toId + "], count: " + (toId - fromId + 1));
        } else {
            log.info("selectAudit: " + tableName + ", age: [" + ageFrom + ".." + ageTo + "], z_id: empty");
        }

        // ����� � ��������� ��������� ���������: id >= fromId � id <= toId
        String query = getSql(tableFrom, tableFields, fromId, toId);

        //
        return db.openSql(query);
    }

    /**
     * ����������, �� ����� ID ������� ������ ����������� ������� � ��������� age
     *
     * @param tableFrom ��� ����� �������
     * @param age       ������� ��
     * @return id ������� ������, ��������������� �������� age
     */
    protected long getAuditMaxIdByAge(IJdxTableStruct tableFrom, long age) throws Exception {
        String query = "select " + JdxUtils.prefix + "id as id from " + JdxUtils.sys_table_prefix + "age where age=" + age + " and table_name='" + tableFrom.getName() + "'";
        return db.loadSql(query).getCurRec().getValueLong("id");
    }

    protected String getSql(IJdxTableStruct tableFrom, String tableFields, long fromId, long toId) {
        return "select " + JdxUtils.prefix + "opr_type, " + tableFields + " from " + JdxUtils.audit_table_prefix + tableFrom.getName() + " where " + JdxUtils.prefix + "id >= " + fromId + " and " + JdxUtils.prefix + "id <= " + toId + " order by " + JdxUtils.prefix + "id";
    }


}
