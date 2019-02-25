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


    public void createAudit() throws Exception {
        String sql;
        DbQuery query = null;
        try {
            log.info("createAudit - ��������� �������");

            // ������� table_list �� ������� �������� ������, �� ����������� � ������� ���� �������
            sql = "create table " + JdxUtils.sys_table_prefix + "table_list(id int not null, name varchar(150) default '' not null)";
            db.execSql(sql);
            // ��������� ����
            sql = "alter table " + JdxUtils.sys_table_prefix + "table_list add constraint pk_" + JdxUtils.audit_table_prefix + "tableList_id primary key (id)";
            db.execSql(sql);
            // ��������� Id ��� ����� �������
            sql = "create generator " + JdxUtils.sys_gen_prefix + "table_list";
            db.execSql(sql);
            sql = "set generator " + JdxUtils.sys_gen_prefix + "table_list to 0";
            db.execSql(sql);

            // ������� � ������ ������ ���������
            sql = "create table " + JdxUtils.sys_table_prefix + "flag_tab(id integer not null, trigger_flag integer not null)";
            db.execSql(sql);
            sql = "insert into " + JdxUtils.sys_table_prefix + "flag_tab(id, trigger_flag) values (1, 1)";
            db.execSql(sql);


            // ������� ��������� �������: ��� �������� �������� ��������� ������, ����������� ������ � �.�.
            sql = "create table " + JdxUtils.sys_table_prefix + "state(id integer not null, que_out_age_done int not null, que_in_no_done int not null)";
            db.execSql(sql);
            sql = "insert into " + JdxUtils.sys_table_prefix + "state(id, que_out_age_done, que_in_no_done) values (1, 0, 0)";
            db.execSql(sql);
            // ������� ��������� ������� �������: ��� �������� �������� ��������� ������, ����������� ������ � �.�.
            sql = "create table " + JdxUtils.sys_table_prefix + "state_ws(id integer not null, ws_id int not null, que_common_no_done int not null, que_in_age_done int not null)";
            db.execSql(sql);
            sql = "create generator " + JdxUtils.sys_gen_prefix + "state_ws";
            db.execSql(sql);

            // ������ ������� �������
            sql = "create table " + JdxUtils.sys_table_prefix + "workstation_list(id integer not null, parent integer not null, ws_name varchar(50) not null)";
            db.execSql(sql);
            // ��������� Id ��� ������ ������� �������
            sql = "create generator " + JdxUtils.sys_gen_prefix + "workstation_list";
            db.execSql(sql);

            // ��������� ������� �������
            sql = "create table " + JdxUtils.sys_table_prefix + "workstation_state(id integer not null, que_out_send int not null)";
            db.execSql(sql);
            // ��������� Id ��� ������ ������� �������
            sql = "create generator " + JdxUtils.sys_gen_prefix + "workstation_state";
            db.execSql(sql);

            // ������� ������
            sql = "create table " + JdxUtils.sys_table_prefix + "que(id integer not null, que_type int not null, ws_id int not null, age int not null)";
            db.execSql(sql);
            sql = "create generator " + JdxUtils.sys_gen_prefix + "que";
            db.execSql(sql);
            sql = "set generator " + JdxUtils.sys_gen_prefix + "que to 0";
            db.execSql(sql);

            // ������� ��� �������� �������� ������
            sql = "create table " + JdxUtils.sys_table_prefix + "age(age int not null, table_name varchar(50) not null, " + JdxUtils.audit_table_prefix + "id int not null, dt timestamp not null)";
            db.execSql(sql);


            // ��������� � ��������� ������� �������� ���� ������

            log.info("createAudit - ������� ���� ������");

            // ������ �� ������� � ������� table_list �������� ���� ������ �� ����
            query = db.createQuery("insert into " + JdxUtils.sys_table_prefix + "table_list(Id, Name) values (GEN_ID(" + JdxUtils.sys_gen_prefix + "table_list, 1), :Name)");
            ArrayList<IJdxTableStruct> tables = struct.getTables();
            long n = 0;
            for (IJdxTableStruct table : tables) {
                n++;
                log.info("createAudit " + n + "/" + tables.size() + " " + table.getName());

                // ������� � ������� table_list �������� �������
                query.setParams(UtCnv.toMap("Name", table.getName()));
                query.execUpdate();
                // ������� ������� ������� ��������� ��� ������ �������
                createAuditTable(table.getName());
            }

        } finally {
            if (query != null) {
                query.close();
            }
        }
    }

    public void dropAudit() throws Exception {
        String query;

        log.info("dropAudit - ������� ���� ������");

        // ������� ��������� � ������ �������� ������� ������� ���������
        ArrayList<IJdxTableStruct> tables = struct.getTables();
        long n = 0;
        for (IJdxTableStruct table : tables) {
            n++;
            log.info("dropAudit " + n + "/" + tables.size() + " " + table.getName());
            //
            dropAuditTable(table.getName());
        }

        log.info("dropAudit - ��������� �������");

        // ������� ��������� �������:
        // ��� �������� ���������, ������ ������� �������,
        // ��������� � �������� �������,
        // ������� ��� �������� �������� ����,
        // ������� � ������ ������ ���������
        String[] jdx_sys_tables = new String[]{"state", "state_ws", "workstation_list", "workstation_state", "que", "age", "table_list", "flag_tab"};
        for (String jdx_sys_table : jdx_sys_tables) {
            try {
                // ������� ������� ��� �������� ���������
                query = "drop table " + JdxUtils.sys_table_prefix + jdx_sys_table;
                db.execSql(query);
            } catch (Exception e) {
                // ���� ��������� ������ �� ����� ������, ��������� ��������� ������
                if (!e.getCause().toString().contains("does not exist")) {
                    System.out.println(e.getMessage());
                    System.out.println(e.getCause().toString());
                }
            }
        }

        // ������� ��������� ����������
        String[] jdx_sys_generators = new String[]{"state", "state_ws", "workstation_list", "workstation_state", "que", "table_list"};
        for (String jdx_sys_generator : jdx_sys_generators) {
            try {
                query = "drop generator " + JdxUtils.sys_gen_prefix + jdx_sys_generator;
                db.execSql(query);
            } catch (Exception e) {
                // ���� ��������� ������ �� ����� ������, ��������� ��������� ������
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
        // ��������� ������ �� �������� ������� ������� ���������
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

        // ��������� Id ��� ����� �������
        query = "create generator " + JdxUtils.audit_gen_prefix + tableName;
        db.execSql(query);
        query = "set generator " + JdxUtils.audit_gen_prefix + tableName + " to 0";
        db.execSql(query);

        // ������ �� ������� ������
        query = createTrigger(tableName, fields, updMods.INSERT);
        //System.out.println(query);
        db.execSqlNative(query);
        // ������ �� ���������� ������
        query = createTrigger(tableName, fields, updMods.UPDATE);
        db.execSqlNative(query);
        // ������ �� �������� ������
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
        // ������������ ����� ��� ������� � ��� ��������
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + fields.get(i).getName() + ", ";
        }
        sql = sql + fields.get(fieldCount - 1).getName() + ") ";
        // ���� ��������
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
            // ������� ��������
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_I";
            db.execSql(sql);
        } catch (Exception ex) {
            // ���� ��������� ������� �� ����� ������, ��������� ��������� ������
        }
        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_D";
            db.execSql(sql);
        } catch (Exception ex) {
            // ���� ��������� ������� �� ����� ������, ��������� ��������� ������
        }
        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_U";
            db.execSql(sql);
        } catch (Exception ex) {
            // ���� ��������� ������� �� ����� ������, ��������� ��������� ������
        }
        try {
            // ������� ���� ������� ������� ���������
            sql = "drop table " + JdxUtils.audit_table_prefix + tableName;
            db.execSql(sql);
            // ������� ��������� ��� ������� ������� ���������
            sql = "drop generator " + JdxUtils.audit_gen_prefix + tableName;
            db.execSql(sql);
        } catch (Exception ex) {
            // ���� ��������� ������� �� ����� �������, ��������� ��������� ������
        }
    }

    public long addWorkstation() throws Exception {
        DbUtils dbu = new DbUtils(db, struct);

        //
        long wsId = dbu.getNextGenerator(JdxUtils.sys_gen_prefix + "workstation_list");
        String sql = "insert into " + JdxUtils.sys_table_prefix + "workstation_list(id, parent, ws_name) values (" + wsId + ", 0, '')";
        db.execSql(sql);

        //
        long id = dbu.getNextGenerator(JdxUtils.sys_gen_prefix + "state_ws");
        sql = "insert into " + JdxUtils.sys_table_prefix + "state_ws(id, ws_id, que_common_no_done, que_in_age_done) values (" + id + ", " + wsId + ", 0, 0)";
        db.execSql(sql);

        //
        return wsId;
    }

}
