package jdtx.repl.main.api;

import jandcode.dbm.db.*;

/**
 * Состояние рабоей станции: насколько отбаботаны очереди и прочее.
 */
public class JdxStateManager {

    Db db;

    public JdxStateManager(Db db) {
        this.db = db;
    }


    /**
     * @return Возраст, до которого сформирована исходящая очередь
     */
    public long getAuditAgeDone() throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state";
        return db.loadSql(sql).getCurRec().getValueLong("que_out_age_done");
    }

    public void setAuditAgeDone(long queOutAgeDone) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state set que_out_age_done = " + queOutAgeDone;
        db.execSql(sql);
    }


    /**
     * @return Номер реплики, до которого обработана входящая очередь
     */
    public long getQueInIdxDone() throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state";
        return db.loadSql(sql).getCurRec().getValueLong("que_in_id_done");
    }

    public void setQueInIdxDone(long queInIdDone) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state set que_in_id_done = " + queInIdDone;
        db.execSql(sql);
    }


}