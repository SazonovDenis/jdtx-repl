package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;

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
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "state";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_out_age_done");
    }

    /**
     * Задает возраст аудита, до которого сформирована исходящая очередь
     * (заполняется при выкладывании реплики в исходящую очередь).
     */
    public void setAuditAgeDone(long queOutAgeDone) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "state set que_out_age_done = " + queOutAgeDone;
        db.execSql(sql);
    }


    /**
     * @return Номер реплики, до которого обработана (применена) очередь (queIn и queIn001)
     */
    public void setQueNoDone(String queName, long queInNoDone) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "state set que_" + queName + "_no_done = " + queInNoDone;
        db.execSql(sql);
    }

    public long getQueNoDone(String queName) throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "state";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no_done");
    }

}