package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;

/**
 * Состояние задач рабочей станции: отметки (в БД), насколько отработаны очереди и прочее.
 */
public class JdxStateManagerWs {

    private Db db;

    public JdxStateManagerWs(Db db) {
        this.db = db;
    }


    /**
     * @return Возраст аудита, до которого сформирована исходящая очередь
     */
    public long getAuditAgeDone() throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_out_age_done");
    }

    /**
     * Задает возраст аудита, до которого сформирована исходящая очередь
     * (заполняется при выкладывании реплики в исходящую очередь).
     */
    public void setAuditAgeDone(long queOutAgeDone) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state set que_out_age_done = " + queOutAgeDone;
        db.execSql(sql);
    }


    /**
     * @return Номер реплики, до которого обработана (применена) входящая очередь
     */
    public long getQueInNoDone() throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_in_no_done");
    }

    public void setQueInNoDone(long queInNoDone) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state set que_in_no_done = " + queInNoDone;
        db.execSql(sql);
    }


}
