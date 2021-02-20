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
        String sqlUpd = "update " + JdxUtils.SYS_TABLE_PREFIX + "state_ws set que_in_age_done = " + queInAgeDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     * @return Отметка: номер реплики, до которого обработана общая очередь
     * при тиражировании реплик для рабочей станции wsId
     */
    public long getDispatchDoneQueCommon(long wsId) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_common_dispatch_done");
    }

    public void setDispatchDoneQueCommon(long wsId, long queCommonNoDone) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + JdxUtils.SYS_TABLE_PREFIX + "state_ws set que_common_dispatch_done = " + queCommonNoDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


/*
    */
/**
     * @return Отметка: до какого номера распределена очередь Out001 для рабочей станции wsId
     *//*

    public long getDispatchDoneQueOut001(long wsId) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_out001_dispatch_done");
    }

    public void setDispatchDoneQueOut001(long wsId, long que001NoDone) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + JdxUtils.sys_table_prefix + "state_ws set que_out001_dispatch_done = " + que001NoDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }
*/


    /**
     * @return Отметка: до какого номера отправлена очередь queName для рабочей станции wsId
     */
    public long getMailSendDone(long wsId, String queName) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_" + queName + "_send_done");
    }

    public void setMailSendDone(long wsId, String queName, long queNoDone) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + JdxUtils.SYS_TABLE_PREFIX + "state_ws set que_" + queName + "_send_done = " + queNoDone + " where ws_id = " + wsId;
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
        String sqlUpd = "update " + JdxUtils.SYS_TABLE_PREFIX + "state_ws set snapshot_age = " + snapshotAge + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     *
     */
    private DataRecord loadRecStateWs(long wsId) throws Exception {
        String sql = "select * from " + JdxUtils.SYS_TABLE_PREFIX + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.SYS_TABLE_PREFIX + "state_ws");
        }
        return rec;
    }


}
