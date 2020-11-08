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
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_in_age_done");
    }

    public void setWsQueInAgeDone(long wsId, long queInAgeDone) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + JdxUtils.sys_table_prefix + "state_ws set que_in_age_done = " + queInAgeDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     * @return Отметка: номер реплики, до которого обработана (разослана) общая очередь
     * при тиражировании реплик для рабочей станции wsId
     */
    public long getQueCommonDispatchDone(long wsId) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_common_dispatch_done");
    }

    public void setQueCommonDispatchDone(long wsId, long queCommonNoDone) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + JdxUtils.sys_table_prefix + "state_ws set que_common_dispatch_done = " + queCommonNoDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     * @return Отметка: до какого номера разослана очередь Out001 для рабочей станции wsId
     */
    public long getQueOut001DispatchDone(long wsId) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_out001_dispatch_done");
    }

    public void setQueOut001DispatchDone(long wsId, long que001NoDone) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + JdxUtils.sys_table_prefix + "state_ws set que_out001_dispatch_done = " + que001NoDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     * @return Номер реплики (из входящей очереди),
     * обработанной на момент создания snapshot-а для рабочей станции wsId
     */
    public long getSnapshotAge(long wsId) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("snapshot_age");
    }

    public void setSnapshotAge(long wsId, long snapshotAge) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + JdxUtils.sys_table_prefix + "state_ws set snapshot_age = " + snapshotAge + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     *
     */
    private DataRecord loadRecStateWs(long wsId) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.sys_table_prefix + "state_ws");
        }
        return rec;
    }


}
