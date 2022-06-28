package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.util.*;

/**
 * Состояние серверных задач: отметки в БД, насколько отработаны очереди и прочее.
 */
public class JdxStateManagerSrv {

    private Db db;

    public JdxStateManagerSrv(Db db) {
        this.db = db;
    }


    /**
     * @return Номер реплики, до которого получена
     * очередь out от рабочей станции и помещена в зеркальную очереди queInSrv.
     */
    public long getWsQueInNoReceived(long wsId) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_in_no");
    }

    /**
     * Отмечает номер реплики, до которого получена
     * очередь out от рабочей станции и помещена в зеркальную очередь queInSrv.
     */
    public void setWsQueInNoReceived(long wsId, long queInNo) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set que_in_no = " + queInNo + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     * @return Номер реплики, до которого очередь out от рабочей станции выложена в общую очередь common
     */
    public long getWsQueInNoDone(long wsId) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_in_no_done");
    }

    /**
     * Устанавливает возраст реплики, до которого обработана
     * очередь out от рабочей станции при формировании общей очереди.
     */
    public void setWsQueInNoDone(long wsId, long queInNoDone) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set que_in_no_done = " + queInNoDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     * @return Номер реплики, до которого обработана общая очередь
     * при тиражировании реплик для рабочей станции wsId.
     */
    public long getDispatchDoneQueCommon(long wsId) throws Exception {
        DataRecord rec = loadRecStateWs(wsId);
        //
        return rec.getValueLong("que_common_dispatch_done");
    }

    /**
     * Устанавливает номер реплики, до которого обработана общая очередь
     * при тиражировании реплик для рабочей станции wsId.
     */
    public void setDispatchDoneQueCommon(long wsId, long queCommonNoDone) throws Exception {
        loadRecStateWs(wsId);
        //
        String sqlUpd = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set que_common_dispatch_done = " + queCommonNoDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


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
        String sqlUpd = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set que_" + queName + "_send_done = " + queNoDone + " where ws_id = " + wsId;
        db.execSql(sqlUpd);
    }


    /**
     *
     */
    private DataRecord loadRecStateWs(long wsId) throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE");
        }
        return rec;
    }


}
