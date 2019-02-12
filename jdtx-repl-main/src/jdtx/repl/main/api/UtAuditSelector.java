package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

public class UtAuditSelector {

    Db db;
    IJdxDbStruct struct;

    public UtAuditSelector(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }


    public void readAuditData(String tableName, String tableFields, long ageFrom, long ageTo, JdxDataWriter dataContainer) throws Exception {
        //
        DbQuery rsTableLog = selectAuditData(tableName, tableFields, ageFrom, ageTo);
        try {
            dataContainer.startTable(tableName);

            // измененные данные помещаем в dataContainer
            while (!rsTableLog.eof()) {
                dataContainer.append();
                // Тип операции
                dataContainer.setOprType(rsTableLog.getValueInt(JdxUtils.prefix + "opr_type"));
                // Тело записи
                String[] tableFromFields = tableFields.split(",");
                for (String field : tableFromFields) {
                    dataContainer.setRecValue(field, rsTableLog.getValue(field));
                }
                //
                rsTableLog.next();
            }

            //
            dataContainer.flush();
        } finally {
            rsTableLog.close();
        }
    }

    public void clearAuditData(long ageFrom, long ageTo) throws Exception {
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
                    System.out.println("clearAudit [" + t.getName() + "], age: [" + ageFrom + ".." + ageTo + "], z_id: [" + fromId + ".." + toId + "], count: " + (toId - fromId + 1));
                } else {
                    System.out.println("clearAudit [" + t.getName() + "], age: [" + ageFrom + ".." + ageTo + "], z_id: empty");
                }

                // изменения с указанным возрастом
                query = "delete from " + JdxUtils.audit_table_prefix + t.getName() + " where " + JdxUtils.prefix + "id >= :fromId and " + JdxUtils.prefix + "id <= :toId";
                db.execSql(query, UtCnv.toMap("fromId", fromId, "toId", toId));
            }
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
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
            System.out.println("selectAudit [" + tableName + "], age: [" + ageFrom + ".." + ageTo + "], z_id: [" + fromId + ".." + toId + "], count: " + (toId - fromId + 1));
        } else {
            System.out.println("selectAudit [" + tableName + "], age: [" + ageFrom + ".." + ageTo + "], z_id: empty");
        }

        // Аудит в указанном диапазоне возрастов: id >= fromId и id <= toId
        String query = getSql(tableFrom, tableFields, fromId, toId);

        //
        return db.openSql(query);
    }

    /**
     * Возвращает, на каком ID таблицы аудита закончилась реплика с возрастом age
     *
     * @param tableFrom для какой таблицы
     * @param age       возраст БД
     * @return id таблицы аудита, соответствующая возрасту age
     */
    protected long getAuditMaxIdByAge(IJdxTableStruct tableFrom, long age) throws Exception {
        String query = "select " + JdxUtils.prefix + "id as id from " + JdxUtils.sys_table_prefix + "age where age=" + age + " and tableName='" + tableFrom.getName() + "'";
        return db.loadSql(query).getCurRec().getValueLong("id");
    }

    protected String getSql(IJdxTableStruct tableFrom, String tableFields, long fromId, long toId) {
        return "select " + JdxUtils.prefix + "opr_type, " + tableFields + " from " + JdxUtils.audit_table_prefix + tableFrom.getName() + " where " + JdxUtils.prefix + "id >= " + fromId + " and " + JdxUtils.prefix + "id <= " + toId + " order by " + JdxUtils.prefix + "id";
    }



}
