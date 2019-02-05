package jdtx.repl.main.api;

import jandcode.dbm.db.*;

public class UtDbObjectDecodeManager {

    Db db = null;

    public UtDbObjectDecodeManager(Db db) {
        this.db = db;
    }

    public void createRefDecodeObject() throws Exception {
        db.execSql("create table " + JdxUtils.sys_table_prefix + "decode (db_code integer, table_name varchar(150), db_slot integer, own_slot integer)");
        db.execSql("create unique index " + JdxUtils.sys_table_prefix + "decode_idx1 on " + JdxUtils.sys_table_prefix + "decode (db_code, table_name, db_slot)");
        db.execSql("create unique index " + JdxUtils.sys_table_prefix + "decode_idx2 on " + JdxUtils.sys_table_prefix + "decode (table_name, own_slot)");
        db.execSql("create table " + JdxUtils.sys_table_prefix + "db_info (db_code integer)");
    }

    public void dropRefDecodeObject() throws Exception {
        String query = "drop table " + JdxUtils.sys_table_prefix + "decode";
        try {
            db.execSql(query);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (!e.getCause().toString().contains("does not exist")) {
                System.out.println(e.getMessage());
                System.out.println(e.getCause().toString());
            }
        }

        //
        try {
            db.execSql("drop table " + JdxUtils.sys_table_prefix + "db_info");
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (!e.getCause().toString().contains("does not exist")) {
                System.out.println(e.getMessage());
                System.out.println(e.getCause().toString());
            }
        }
    }

}
