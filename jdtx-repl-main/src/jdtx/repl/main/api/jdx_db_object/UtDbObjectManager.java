package jdtx.repl.main.api.jdx_db_object;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.pk_generator.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.util.*;

public class UtDbObjectManager implements IDbObjectManager {

    public int CURRENT_VER_DB = 17;

    enum IDEmodes {INSERT, UPDATE, DELETE}

    Db db;
    IDbNamesManager dbNamesManager;
    IDbErrors dbErrors;
    IDbGenerators dbGenerators;

    public UtDbObjectManager(Db db) {
        this.db = db;
        this.dbNamesManager = DbToolsService.getDbNamesManager(db);
        this.dbErrors = DbToolsService.getDbErrors(db);
        this.dbGenerators = DbToolsService.getDbGenerators(db);
    }

    DbQuery lockFlag = null;

    static Log log = LogFactory.getLog("jdtx.DbObject");

    public void checkVerDb() throws Exception {
        // --- Проверяем, что можно прочитать версию БД
        initVerDb();

        // --- Запрещаем другим читать и менять версию
        lockDb();

        // --- Читаем и, если нужно, обновляем версию
        try {
            // Читаем версию БД
            DataRecord rec = db.loadSql("select ver, ver_step from " + UtJdx.SYS_TABLE_PREFIX + "verdb").getCurRec();
            int ver = rec.getValueInt("ver");
            int ver_step = rec.getValueInt("ver_step");

            // Проверяем версию, что не больше разрешенной
            if (ver > CURRENT_VER_DB || (ver == CURRENT_VER_DB && ver_step != 0)) {
                throw new XError("Версия базы больше разрешенной, текущая: " + ver + "." + ver_step + ", разрешенная: " + CURRENT_VER_DB + ".0");
            }

            // Обновляем версию
            int ver_to = CURRENT_VER_DB;
            int ver_i = ver;
            if (ver_i < ver_to) {
                while (ver_i < ver_to) {
                    log.info("Смена версии: " + ver_i + "." + ver_step + " -> " + (ver_i + 1) + ".0");

                    //
                    String updateFileName = "update_" + UtString.padLeft(String.valueOf(ver_i), 3, "0") + "_" + UtString.padLeft(String.valueOf(ver_i + 1), 3, "0") + ".sql";
                    String sqls = UtFile.loadString("res:jdtx/repl/main/api/jdx_db_object/" + updateFileName);

                    //
                    String[] sqlArr = sqls.split(";");
                    for (int ver_step_i = ver_step; ver_step_i < sqlArr.length; ) {
                        String sql = sqlArr[ver_step_i].trim();
                        if (sql.length() == 0) {
                            ver_step_i = ver_step_i + 1;
                            continue;
                        }
                        //
                        log.info("Смена версии, шаг: " + ver_i + "." + ver_step_i);
                        //
                        if (sql.startsWith("@")) {
                            SqlScriptExecutorService svc = db.getApp().service(SqlScriptExecutorService.class);
                            ISqlScriptExecutor script = svc.createByName(sql.substring(1));
                            script.exec(db);
                        } else {
                            db.execSql(sql);
                        }
                        //
                        ver_step_i = ver_step_i + 1;
                        setVerDb(ver_i, ver_step_i);
                    }

                    //
                    ver_i = ver_i + 1;
                    ver_step = 0;
                    setVerDb(ver_i, ver_step);

                    //
                    log.info("Смена версии до: " + ver_i + "." + ver_step + " - ok");
                }
            }

        } finally {
            // Снимаем блокировку
            unlockDb();
        }

    }

    public void checkReplicationInit() throws Exception {
        try {
            // Читаем код нашей станции
            DataRecord rec = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO").getCurRec();
            // Проверяем код нашей станции
            if (rec.getValueLong("ws_id") == 0) {
                throw new XError("Invalid workstation.ws_id == 0");
            }
        } catch (Exception e) {
            if (dbErrors.errorIs_TableNotExists(e)) {
                throw new XError("Replication is not initialized: " + e.getMessage());
            } else {
                throw e;
            }
        }
    }

    public void createReplBase(long wsId, String guid) throws Exception {
        log.info("Создаем системные структуры");

        // Базовая структура для verdb
        initVerDb();

        // Базовая структура (системные структуры)
        String verDbStr = UtString.padLeft(String.valueOf(CURRENT_VER_DB), 3, '0');
        OutBuilder builder = new OutBuilder(db.getApp());
        Map args = new HashMap();
        args.put("SYS_TABLE_PREFIX", UtJdx.SYS_TABLE_PREFIX);
        args.put("SYS_GEN_PREFIX", UtJdx.SYS_GEN_PREFIX);
        builder.outTml("res:jdtx/repl/main/api/jdx_db_object/create_" + verDbStr + "." + UtJdx.getDbType(db) + ".sql", args, null);
        String sql = builder.toString();
        //
        JdxDbUtils.execScript(sql, db);

        // Базовая структура для verdb
        setVerDb(CURRENT_VER_DB, 0);

        // Метка с guid БД и номером wsId
        log.info("Помечаем рабочую станцию, ws_id: " + wsId + ", guid: " + guid);
        sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO set ws_id = " + wsId + ", guid = '" + guid + "'";
        db.execSql(sql);
    }

    public void dropReplBase() throws Exception {
        // Удаляем системные таблицы и генераторы
        String[] jdx_sys_tables = new String[]{
                "db_info", "workstation_list", "state", "state_ws", "workstation", "que_common", "que_out000", "que_out001", // старые, но тоже удаляем
                "verdb",
                "age", "flag_tab", "ws_info", "ws_state", "srv_state", "srv_workstation_state", "srv_workstation_list",
                "srv_que_in", "srv_que_common", "srv_que_out001", "srv_que_out000",
                "que_in001", "que_in", "que_out"
        };
        dropSysTables(jdx_sys_tables);
    }

    public void createAudit(IJdxTable table) throws Exception {
        // Создание таблицы журнала
        String tableName = table.getName();
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        String auditTableName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TABLE_PREFIX);
        //
        String pkFieldDataType = table.getPrimaryKey().get(0).getDbDatatype();
        if (table.getPrimaryKey().get(0).getJdxDatatype() == JdxDataType.STRING) {
            pkFieldDataType = pkFieldDataType + "(" + table.getPrimaryKey().get(0).getSize() + ")";
        }
        String sql = "create table \n" +
                auditTableName + "(\n" +
                UtJdx.AUDIT_FIELD_PREFIX + "id integer not null,\n" +
                UtJdx.SQL_FIELD_OPR_TYPE + " integer not null,\n" +
                UtJdx.AUDIT_FIELD_PREFIX + "opr_dttm timestamp not null,\n" +
                pkFieldName + " " + pkFieldDataType + " not null\n" +
                ")";
        try {
            db.execSql(sql);
        } catch (Exception e) {
            if (dbErrors.errorIs_TableAlreadyExists(e)) {
                log.warn("createAuditTable, audit table already exists, table: " + table.getName());
            } else {
                throw e;
            }
        }

        // Генератор Id для новой таблицы
        String generatorName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_GEN_PREFIX);
        dbGenerators.createGenerator(generatorName);

        // Индекс для таблицы журнала
        createAuditTableIndex_ID(table);
        createAuditTableIndex_OPR_DTTM(table);

        // Тригер на вставку записи
        createAuditTrigger(table, IDEmodes.INSERT);

        // Тригер на обновление записи
        createAuditTrigger(table, IDEmodes.UPDATE);

        // Тригер на удаление записи
        createAuditTrigger(table, IDEmodes.DELETE);
    }

    public void dropAudit(String tableName) throws Exception {
        String sql;

        // удаляем триггеры

        String triggerName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TRIGER_PREFIX, "_I");
        try {
            sql = "drop trigger " + triggerName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (dbErrors.errorIs_TriggerNotExists(e)) {
                log.debug("dropAudit, audit trigger for " + IDEmodes.INSERT + " not exists, table: " + tableName + ", trigger: " + triggerName);
            } else {
                throw e;
            }
        }

        triggerName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TRIGER_PREFIX, "_U");
        try {
            sql = "drop trigger " + triggerName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (dbErrors.errorIs_TriggerNotExists(e)) {
                log.debug("dropAudit, audit trigger for " + IDEmodes.UPDATE + " not exists, table: " + tableName + ", trigger: " + triggerName);
            } else {
                throw e;
            }
        }

        triggerName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TRIGER_PREFIX, "_D");
        try {
            sql = "drop trigger " + triggerName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (dbErrors.errorIs_TriggerNotExists(e)) {
                log.debug("dropAudit, audit trigger for " + IDEmodes.DELETE + " not exists, table: " + tableName + ", trigger: " + triggerName);
            } else {
                throw e;
            }
        }

        // удаляем саму таблицу журнала изменений
        String auditTableName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TABLE_PREFIX);
        try {
            sql = "drop table " + auditTableName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (dbErrors.errorIs_TableNotExists(e)) {
                log.debug("dropAudit, audit table not exists, table: " + tableName + ", audit: " + auditTableName);
            } else {
                throw e;
            }
        }

        // удаляем генератор для таблицы журнала изменений
        String generatorName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_GEN_PREFIX);
        dbGenerators.dropGenerator(generatorName);
    }

    public void lockDb() throws Exception {
        if (lockFlag != null) {
            throw new XError("Database already locked by current thread");
        }

        //
        log.info("Wait for lock database: " + db.getDbSource().getDatabase());
        try {
            lockFlag = db.openSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "verdb for update with lock");
        } catch (Exception e) {
            if (e.getMessage().contains("deadlock") && e.getMessage().contains("update conflicts with concurrent update")) {
                throw new XError("Database lock error: " + UtJdxErrors.message_failedDatabaseLock);
            }
        }

        //
        log.info("Database locked: " + db.getDbSource().getDatabase());
    }

    public void unlockDb() throws Exception {
        if (lockFlag == null) {
            throw new XError("Database is not locked by current thread");
        }

        //
        lockFlag.close();
        lockFlag = null;

        //
        log.info("Database unlocked: " + db.getDbSource().getDatabase());
    }


    // ---
    // Утилиты
    // ---

    private void initVerDb() throws Exception {
        try {
            db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "verdb").getCurRec();
        } catch (Exception e) {
            if (dbErrors.errorIs_TableNotExists(e)) {
                // Создаем таблицу verdb
                log.info("Создаем таблицу " + UtJdx.SYS_TABLE_PREFIX + "verdb");
                //
                OutBuilder builder = new OutBuilder(db.getApp());
                Map args = new HashMap();
                args.put("SYS_TABLE_PREFIX", UtJdx.SYS_TABLE_PREFIX);
                args.put("SYS_GEN_PREFIX", UtJdx.SYS_GEN_PREFIX);
                builder.outTml("res:jdtx/repl/main/api/jdx_db_object/UtDbObjectManager.verdb.sql", args, null);
                String sql = builder.toString();
                //
                JdxDbUtils.execScript(sql, db);
            } else {
                throw e;
            }
        }
    }

    private void setVerDb(int ver, int ver_step) throws Exception {
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "verdb set ver = " + ver + ", ver_step = " + ver_step);
    }

/*
    private void createAuditTriggers_full(IJdxTable table) throws Exception {
        // Тригер на вставку записи
        createTrigger_full(table, IDEmodes.INSERT);

        // Тригер на обновление записи
        createTrigger_full(table, IDEmodes.UPDATE);

        // Тригер на удаление записи
        createTrigger_full(table, IDEmodes.DELETE);
    }
*/

    private void createAuditTrigger(IJdxTable table, IDEmodes IDEmode) throws Exception {
        try {
            String sql = getSqlCreateAuditTrigger(table, IDEmode);
            db.execSqlNative(sql);
        } catch (Exception e) {
            if (dbErrors.errorIs_TriggerAlreadyExists(e)) {
                log.warn("createAuditTriggers, trigger " + IDEmode + " already exists, table: " + table.getName());
            } else {
                throw e;
            }
        }
    }

/*
    private void createTrigger_full(IJdxTable table, IDEmodes IDEmode) throws Exception {
        try {
            String sql = getSqlCreateTrigger_full(table, IDEmode);
            db.execSqlNative(sql);
        } catch (Exception e) {
            if (dbErrors.errorIs_TriggerAlreadyExists(e)) {
                log.warn("createAuditTriggers, trigger " + IDEmode + " already exists, table: " + table.getName());
            } else {
                throw e;
            }
        }
    }
*/

    void createAuditTableIndex_ID(IJdxTable table) throws Exception {
        String tableName = table.getName();
        String auditTableName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TABLE_PREFIX);
        String idxName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_INDEX_PREFIX, "_IDX");
        try {
            String sql = "CREATE UNIQUE INDEX " + idxName + " ON " + auditTableName + " (" + UtJdx.AUDIT_FIELD_PREFIX + "ID)";
            db.execSql(sql);
        } catch (Exception e) {
            if (dbErrors.errorIs_IndexAlreadyExists(e)) {
                log.debug("createAuditTableIndex_ID, index already exists, index: " + idxName + ", table: " + table.getName());
            } else {
                throw e;
            }
        }
    }

    void createAuditTableIndex_OPR_DTTM(IJdxTable table) throws Exception {
        String tableName = table.getName();
        String auditTableName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TABLE_PREFIX);
        String idxName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_INDEX_PREFIX, "_DT");
        try {
            String sql = "CREATE INDEX " + idxName + " ON " + auditTableName + " (" + UtJdx.AUDIT_FIELD_PREFIX + "OPR_DTTM)";
            db.execSql(sql);
        } catch (Exception e) {
            if (dbErrors.errorIs_IndexAlreadyExists(e)) {
                log.warn("createAuditTableIndex_OPR_DTTM, index already exists, index: " + idxName + ", table: " + table.getName());
            } else {
                throw e;
            }
        }
    }

/*
    private void createAuditTable_full(IJdxTable table) throws Exception {
        String tableName = table.getName();
        List<IJdxField> fields = table.getFields();

        // формируем запрос на создание таблицы журнала изменений
        int fieldCount = fields.size();
        String fieldsStr = "";
        for (int i = 0; i < fieldCount; i++) {
            if (fieldsStr.length() != 0) {
                fieldsStr = fieldsStr + ", ";
            }

            fieldsStr = fieldsStr + fields.get(i).getName() + " " + fields.get(i).getDbDatatype();
            if (JdxDbStructReader.isStringField(fields.get(i).getDbDatatype())) {
                fieldsStr = fieldsStr + "(" + fields.get(i).getSize() + ")";
            }
        }

        String sql = "create table " + UtJdx.AUDIT_TABLE_PREFIX + tableName + "(" +
                UtJdx.AUDIT_TABLE_PREFIX + "id integer not null, " +
                UtJdx.SQL_FIELD_OPR_TYPE + " integer not null, " +
                fieldsStr +
                ")";

        db.execSql(sql);

        // генератор Id для новой таблицы
        String generatorName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_GEN_PREFIX);
        createGenerator(generatorName);
    }
*/

    private String getSqlCreateAuditTrigger(IJdxTable table, IDEmodes upd_mode) {
        String sql;
        String tableName = table.getName();
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        String triggerName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TRIGER_PREFIX, "_" + upd_mode.toString().substring(0, 1));
        String generatorName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_GEN_PREFIX);
        String audditTableName = dbNamesManager.getShortName(tableName, UtJdx.AUDIT_TABLE_PREFIX);
        //
        sql = "create trigger " + triggerName + " for " + tableName + " after " + upd_mode.toString() + " \n" +
                "as\n" +
                "declare variable trigger_flag_ integer;\n" +
                "declare variable opr_dttm_ timestamp;\n" +
                "begin\n" +
                "  select trigger_flag from " + UtJdx.SYS_TABLE_PREFIX + "flag_tab where id=1 into :trigger_flag_;\n" +
                "  select current_timestamp from rdb$database into :opr_dttm_;\n" +
                "  if (trigger_flag_<>0) then\n" +
                "  begin\n" +
                "    insert into " + audditTableName + " (" + UtJdx.AUDIT_FIELD_PREFIX + "id, " + UtJdx.SQL_FIELD_OPR_TYPE + ", " + UtJdx.AUDIT_FIELD_PREFIX + "opr_dttm, " + pkFieldName + ") ";
        // значения
        sql = sql + "values (GEN_ID(" + generatorName + ", 1), " + (upd_mode.ordinal() + 1) + ", :opr_dttm_, ";
        //
        String contVar = "NEW.";
        if (upd_mode.toString().equals("DELETE")) {
            contVar = "OLD.";
        }
        sql = sql + contVar + pkFieldName;
        sql = sql + ");\n";
        //
        sql = sql + "  end\nend";
        return sql;
    }

/*
    private String getSqlCreateTrigger_full(IJdxTable table, IDEmodes upd_mode) {
        String tableName = table.getName();
        String sql;
        int fieldCount = table.getFields().size();
        sql = "create trigger " + UtJdx.TRIGER_PREFIX + tableName + "_" + upd_mode.toString().substring(0, 1) + " for " + tableName + " after " + upd_mode.toString() + " \n" +
                "as\n" +
                "declare variable trigger_flag_ integer;\n" +
                "begin\n" +
                "  select trigger_flag from " + UtJdx.SYS_TABLE_PREFIX + "flag_tab where id=1 into :trigger_flag_;\n" +
                "  if (trigger_flag_<>0) then\n" +
                "  begin\n" +
                "    insert into " + UtJdx.AUDIT_TABLE_PREFIX + tableName + " (" + UtJdx.AUDIT_TABLE_PREFIX + "id, " + UtJdx.SQL_FIELD_OPR_TYPE + ", ";
        // перечисление полей для вставки в них значений
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + table.getFields().get(i).getName() + ", ";
        }
        sql = sql + table.getFields().get(fieldCount - 1).getName() + ") ";
        // сами значения
        sql = sql + "values (GEN_ID(" + UtJdx.AUDIT_GEN_PREFIX + tableName + ", 1), " + (upd_mode.ordinal() + 1) + ", ";
        String contVar = "NEW.";
        if (upd_mode.toString().equals("DELETE")) {
            contVar = "OLD.";
        }
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + contVar + table.getFields().get(i).getName() + ", ";
        }
        sql = sql + contVar + table.getFields().get(fieldCount - 1).getName() + "); \n";
        sql = sql + "  end\nend";
        return sql;
    }
*/

    void dropSysTables(String[] sys_names) throws Exception {
        // удаляем генераторы
        for (String name : sys_names) {
            dbGenerators.dropGenerator(UtJdx.SYS_GEN_PREFIX + name);
        }

        // удаляем таблицу
        for (String name : sys_names) {
            dropTable(UtJdx.SYS_TABLE_PREFIX + name);
        }
    }

    void dropTable(String tableName) throws Exception {
        try {
            String sql = "drop table " + tableName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (dbErrors.errorIs_TableNotExists(e)) {
                log.debug("table not exists: " + tableName);
            } else {
                throw e;
            }
        }
    }

}
