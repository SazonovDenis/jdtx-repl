package jdtx.repl.main.api;


import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

import java.sql.*;
import java.util.*;

public class UtDbObjectManager {

    IJdxDbStruct struct;
    Db db;

    protected static Log log = LogFactory.getLog("jdtx");

    enum updMods {INSERT, UPDATE, DELETE}


    public UtDbObjectManager(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }


    public void createAudit(long wsId) throws Exception {
        String sql;
        DbQuery query = null;
        try {
            log.info("createAudit - системные объекты");

            // таблица table_list со списком названий таблиц, за изменениями в которых надо следить
            sql = "create table " + JdxUtils.sys_table_prefix + "table_list(id int not null, name varchar(150) default '' not null)";
            db.execSql(sql);
            sql = "alter table " + JdxUtils.sys_table_prefix + "table_list add constraint pk_" + JdxUtils.sys_table_prefix + "table_list primary key (id)";
            db.execSql(sql);
            sql = "create generator " + JdxUtils.sys_gen_prefix + "table_list";
            db.execSql(sql);
            sql = "set generator " + JdxUtils.sys_gen_prefix + "table_list to 0";
            db.execSql(sql);

            // таблица с флагом работы триггеров
            sql = "create table " + JdxUtils.sys_table_prefix + "flag_tab(id integer not null, trigger_flag integer not null)";
            db.execSql(sql);
            sql = "insert into " + JdxUtils.sys_table_prefix + "flag_tab(id, trigger_flag) values (1, 1)";
            db.execSql(sql);

            // таблица собственного состояния (для рабочей станции): для хранения возраста созданных реплик, примененных реплик и т.п.
            sql = "create table " + JdxUtils.sys_table_prefix + "state(id integer not null, que_out_age_done int not null, que_in_no_done int not null, mail_send_done int not null)";
            db.execSql(sql);
            sql = "insert into " + JdxUtils.sys_table_prefix + "state(id, que_out_age_done, que_in_no_done, mail_send_done) values (1, 0, 0, 0)";
            db.execSql(sql);

            // таблица состояния рабочих станций (для сервера): для хранения возраста созданных реплик, примененных реплик и т.п.
            sql = "create table " + JdxUtils.sys_table_prefix + "state_ws(id integer not null, ws_id int not null, que_common_dispatch_done int not null, que_in_age_done int not null)";
            db.execSql(sql);
            sql = "alter table " + JdxUtils.sys_table_prefix + "state_ws add constraint pk_" + JdxUtils.sys_table_prefix + "state_ws primary key (id)";
            db.execSql(sql);
            sql = "create generator " + JdxUtils.sys_gen_prefix + "state_ws";
            db.execSql(sql);

            // список рабочих станций
            sql = "create table " + JdxUtils.sys_table_prefix + "workstation_list(id integer not null, name varchar(50) not null)";
            db.execSql(sql);
            // первичный ключ
            sql = "alter table " + JdxUtils.sys_table_prefix + "workstation_list add constraint pk_" + JdxUtils.sys_table_prefix + "workstation_list primary key (id)";
            db.execSql(sql);

            // очереди реплик
            sql = "create table " + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.IN] + "(id integer not null, ws_id int not null, age int not null, replica_type int not null)";
            db.execSql(sql);
            sql = "alter table " + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.IN] + " add constraint pk_" + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.IN] + " primary key (id)";
            db.execSql(sql);
            sql = "create generator " + JdxUtils.sys_gen_prefix + "que" + JdxQueType.table_suffix[JdxQueType.IN];
            db.execSql(sql);
            sql = "set generator " + JdxUtils.sys_gen_prefix + "que" + JdxQueType.table_suffix[JdxQueType.IN] + " to 0";
            db.execSql(sql);
            //
            sql = "create table " + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.OUT] + "(id integer not null, ws_id int not null, age int not null, replica_type int not null)";
            db.execSql(sql);
            sql = "alter table " + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.OUT] + " add constraint pk_" + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.OUT] + " primary key (id)";
            db.execSql(sql);
            sql = "create generator " + JdxUtils.sys_gen_prefix + "que" + JdxQueType.table_suffix[JdxQueType.OUT];
            db.execSql(sql);
            sql = "set generator " + JdxUtils.sys_gen_prefix + "que" + JdxQueType.table_suffix[JdxQueType.OUT] + " to 0";
            db.execSql(sql);
            //
            sql = "create table " + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.COMMON] + "(id integer not null, ws_id int not null, age int not null, replica_type int not null)";
            db.execSql(sql);
            sql = "alter table " + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.COMMON] + " add constraint pk_" + JdxUtils.sys_table_prefix + "que" + JdxQueType.table_suffix[JdxQueType.COMMON] + " primary key (id)";
            db.execSql(sql);
            sql = "create generator " + JdxUtils.sys_gen_prefix + "que" + JdxQueType.table_suffix[JdxQueType.COMMON];
            db.execSql(sql);
            sql = "set generator " + JdxUtils.sys_gen_prefix + "que" + JdxQueType.table_suffix[JdxQueType.COMMON] + " to 0";
            db.execSql(sql);

            // таблица для хранения возраста таблиц
            sql = "create table " + JdxUtils.sys_table_prefix + "age (age int not null, table_name varchar(50) not null, " + JdxUtils.prefix + "id int not null, dt timestamp not null)";
            db.execSql(sql);


            // Вставляем в таблицу table_list названия всех таблиц
            log.info("createAudit - объекты базы данных");

            // запрос на вставку в таблицу table_list названий всех таблиц из базы
            query = db.createQuery("insert into " + JdxUtils.sys_table_prefix + "table_list (id, Name) values (GEN_ID(" + JdxUtils.sys_gen_prefix + "table_list, 1), :Name)");
            ArrayList<IJdxTableStruct> tables = struct.getTables();
            long n = 0;
            for (IJdxTableStruct table : tables) {
                n++;
                log.info("createAudit " + n + "/" + tables.size() + " " + table.getName());

                // Вставка в таблицу table_list названия таблицы
                query.setParams(UtCnv.toMap("Name", table.getName()));
                query.execUpdate();
                // создаем таблицу журнала изменений для каждой таблицы
                createAuditTable(table.getName());
            }


            // метка с номером БД
            db.execSql("create table " + JdxUtils.sys_table_prefix + "db_info (ws_id integer)");
            sql = "insert into " + JdxUtils.sys_table_prefix + "db_info (ws_id) values (" + wsId + ")";
            db.execSql(sql);
            //
            log.info("db_info, ws_id: " + wsId);
        } finally {
            if (query != null) {
                query.close();
            }
        }
    }

    public void dropAudit() throws Exception {
        String query;

        log.info("dropAudit - объекты базы данных");

        // Удаляем связанную с каждой таблицей таблицу журнала изменений
        ArrayList<IJdxTableStruct> tables = struct.getTables();
        long n = 0;
        for (IJdxTableStruct table : tables) {
            n++;
            log.info("dropAudit " + n + "/" + tables.size() + " " + table.getName());
            //
            dropAuditTable(table.getName());
        }

        log.info("dropAudit - системные объекты");

        // Удаляем системные таблицы:
        // для хранения состояния, список рабочих станций,
        // исходящая и входящая очереди,
        // таблицу для хранения возраста базы,
        // таблицу с флагом работы триггеров,
        // таблицу с меткой БД
        String[] jdx_sys_tables = new String[]{
                "age", "flag_tab", "state", "state_ws", "workstation_list", "table_list", "db_info",
                "que" + JdxQueType.table_suffix[JdxQueType.IN],
                "que" + JdxQueType.table_suffix[JdxQueType.OUT],
                "que" + JdxQueType.table_suffix[JdxQueType.COMMON]
        };
        for (String jdx_sys_table : jdx_sys_tables) {
            try {
                // удаляем таблицу
                query = "drop table " + JdxUtils.sys_table_prefix + jdx_sys_table;
                db.execSql(query);
            } catch (Exception e) {
                // если удаляемый объект не будет найден, программа продолжит работу
                if (!e.getCause().toString().contains("does not exist")) {
                    System.out.println(e.getMessage());
                    System.out.println(e.getCause().toString());
                }
            }
        }

        // Удаляем системные генераторы
        String[] jdx_sys_generators = new String[]{
                "state", "state_ws", "table_list",
                "que" + JdxQueType.table_suffix[JdxQueType.IN],
                "que" + JdxQueType.table_suffix[JdxQueType.OUT],
                "que" + JdxQueType.table_suffix[JdxQueType.COMMON]
        };
        for (String jdx_sys_generator : jdx_sys_generators) {
            try {
                query = "drop generator " + JdxUtils.sys_gen_prefix + jdx_sys_generator;
                db.execSql(query);
            } catch (Exception e) {
                // если удаляемый объект не будет найден, программа продолжит работу
                if (!e.getCause().toString().contains("Generator not found")) {
                    System.out.println(e.getMessage());
                    System.out.println(e.getCause().toString());
                }
            }
        }
    }

    private void createAuditTable(String tableName) throws Exception {
        IJdxTableStruct table = struct.getTable(tableName);
        ArrayList<IJdxFieldStruct> fields = table.getFields();
        int fieldCount = fields.size();
        // формируем запрос на создание таблицы журнала изменений
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

        String query = "create table " + JdxUtils.audit_table_prefix + tableName + "(" +
                JdxUtils.audit_table_prefix + "id integer not null, " +
                JdxUtils.audit_table_prefix + "opr_type integer not null, " +
                JdxUtils.audit_table_prefix + "trigger_flag integer not null, " +
                fieldsStr +
                ")";

        db.execSql(query);

        // генератор Id для новой таблицы
        query = "create generator " + JdxUtils.audit_gen_prefix + tableName;
        db.execSql(query);
        query = "set generator " + JdxUtils.audit_gen_prefix + tableName + " to 0";
        db.execSql(query);

        // тригер на вставку записи
        query = createTrigger(tableName, fields, updMods.INSERT);
        //System.out.println(query);
        db.execSqlNative(query);
        // тригер на обновление записи
        query = createTrigger(tableName, fields, updMods.UPDATE);
        db.execSqlNative(query);
        // тригер на удаление записи
        query = createTrigger(tableName, fields, updMods.DELETE);
        db.execSqlNative(query);
    }

    private String createTrigger(String tableName, ArrayList<IJdxFieldStruct> fields, updMods upd_mode) {
        String sql;
        int fieldCount = fields.size();
        sql = "create trigger " + JdxUtils.trig_pref + tableName + "_" + upd_mode.toString().substring(0, 1) + " for " + tableName + " after " + upd_mode.toString() + " \n" +
                "as\n" +
                "declare variable trigger_flag_ integer;\n" +
                "begin\n" +
                "  select trigger_flag from " + JdxUtils.sys_table_prefix + "flag_tab where id=1 into :trigger_flag_;\n" +
                "  if (trigger_flag_<>0) then\n" +
                "  begin\n" +
                "    insert into " + JdxUtils.audit_table_prefix + tableName + " (" + JdxUtils.audit_table_prefix + "id, " + JdxUtils.audit_table_prefix + "opr_type, " + JdxUtils.audit_table_prefix + "trigger_flag, ";
        // перечисление полей для вставки в них значений
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + fields.get(i).getName() + ", ";
        }
        sql = sql + fields.get(fieldCount - 1).getName() + ") ";
        // сами значения
        sql = sql + "values (GEN_ID(" + JdxUtils.audit_gen_prefix + tableName + ", 1), " + (upd_mode.ordinal() + 1) + ", :trigger_flag_, ";
        String contVar = "NEW.";
        if (upd_mode.toString().equals("DELETE")) {
            contVar = "OLD.";
        }
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + contVar + fields.get(i).getName() + ", ";
        }
        sql = sql + contVar + fields.get(fieldCount - 1).getName() + "); \n";
        sql = sql + "  end\nend";
        return sql;
    }

    private void dropAuditTable(String tableName) throws SQLException {
        //Statement st = conn.createStatement();
        String sql;
        try {
            // удаляем триггеры
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_I";
            db.execSql(sql);
        } catch (Exception ex) {
            // если удаляемый триггер не будет найден, программа продолжит работу
        }
        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_D";
            db.execSql(sql);
        } catch (Exception ex) {
            // если удаляемый триггер не будет найден, программа продолжит работу
        }
        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_U";
            db.execSql(sql);
        } catch (Exception ex) {
            // если удаляемый триггер не будет найден, программа продолжит работу
        }
        try {
            // удаляем саму таблицу журнала изменений
            sql = "drop table " + JdxUtils.audit_table_prefix + tableName;
            db.execSql(sql);
            // удаляем генератор для таблицы журнала изменений
            sql = "drop generator " + JdxUtils.audit_gen_prefix + tableName;
            db.execSql(sql);
        } catch (Exception ex) {
            // если удаляемая таблица не будет найдена, программа продолжит работу
        }
    }

    public void addWorkstation(long wsId, String wsName) throws Exception {
        Map params = new HashMap<>();
        params.put("id", wsId);
        params.put("name", wsName);
        addWorkstation(params);
    }

    private void addWorkstation(Map<String, Object> params) throws Exception {
        DbUtils dbu = new DbUtils(db, struct);

        //
        String sql = "insert into " + JdxUtils.sys_table_prefix + "workstation_list(id, name) values (:id, :name)";
        db.execSql(sql, params);

        //
        long wsId = (long) params.get("id");
        long id = dbu.getNextGenerator(JdxUtils.sys_gen_prefix + "state_ws");
        sql = "insert into " + JdxUtils.sys_table_prefix + "state_ws(id, ws_id, que_common_dispatch_done, que_in_age_done) values (" + id + ", " + wsId + ", 0, 0)";
        db.execSql(sql);
    }

}
