package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;

/**
 * Состояние серверных задач: отметки (в БД), насколько отработаны очереди и прочее.
 */
public class JdxStateManagerSrv {

    private Db db;

    public JdxStateManagerSrv(Db db) {
        this.db = db;
    }


    /**
     * @return Возраст реплики, до которого обработана входящая очередь от рабочей станции
     * при формировании общей очереди
     */
    public long getWsQueInAgeDone(long wsId) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.sys_table_prefix + "state_ws");
        }
        //
        return rec.getValueLong("que_in_age_done");
    }

    public void setWsQueInAgeDone(long wsId, long queInAgeDone) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.sys_table_prefix + "state_ws");
        }
        //
        String sqlUpd = "update " + JdxUtils.sys_table_prefix + "state_ws set que_in_age_done = " + queInAgeDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     * @return Номер реплики, до которого обработана общая очередь
     * при тиражировании реплик для рабочей станции wsId
     */
    public long getCommonQueDispatchDone(long wsId) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.sys_table_prefix + "state_ws");
        }
        //
        return rec.getValueLong("que_common_dispatch_done");
    }

    public void setCommonQueDispatchDone(long wsId, long queCommonNoDone) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.sys_table_prefix + "state_ws");
        }
        //
        String sqlUpd = "update " + JdxUtils.sys_table_prefix + "state_ws set que_common_dispatch_done = " + queCommonNoDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


}
