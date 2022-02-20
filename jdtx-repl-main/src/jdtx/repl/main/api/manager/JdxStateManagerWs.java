package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;

/**
 * Состояние задач рабочей станции: отметки в БД, насколько отработаны очереди и прочее.
 */
public class JdxStateManagerWs {

    private Db db;

    public JdxStateManagerWs(Db db) {
        this.db = db;
    }


    /**
     * @return Возраст аудита, до которого сформирована исходящая очередь QueOut
     */
    public long getAuditAgeDoneQueOut() throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("audit_age_done");
    }

    /**
     * Задает возраст аудита, до которого сформирована исходящая очередь QueOut
     * (заполняется при выкладывании реплики в исходящую очередь).
     */
    public void setAuditAgeDoneQueOut(long auditAgeDone) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE set audit_age_done = " + auditAgeDone;
        db.execSql(sql);
    }


    /**
     * @return Номер реплики, до которого обработана (применена и т.п.) очередь (queIn, queIn001 и т.п.)
     */
    public void setQueNoDone(String queName, long queInNoDone) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE set que_" + queName + "_no_done = " + queInNoDone;
        db.execSql(sql);
    }

    public long getQueNoDone(String queName) throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no_done");
    }

}
