package jdtx.repl.main.api.manager;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

/**
 * Сервер: отметка состояния рабочих станций: MUTE и UMNMUTE.
 */
public class JdxMuteManagerSrv {

    private Db db;

    protected static Log log = LogFactory.getLog("jdtx.MuteManagerSrv");

    public JdxMuteManagerSrv(Db db) {
        this.db = db;
    }

    /**
     * Отметка состояния станции: MUTE
     * Также отмечается номер той реплики в общей очереди queCommon, которой станция отправила подтверждение своего состояния MUTE.
     * Мы можем расчитывать, что пока станция не выйдет из состояния MUTE,
     * новых больше номера commonQueNo реплик в общей очереди queCommon от этой станции не появится.
     */
    public void setMuteDone(long wsId, long queCommonNo) throws Exception {
        log.info("workstation mute done, wsId: " + wsId + ", queCommonNo: " + queCommonNo);
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set mute_age = " + queCommonNo + " where ws_id = " + wsId;
        db.execSql(sql);
    }

    /**
     * Отметка состояния станции: UMNMUTE.
     */
    public void setUnmuteDone(long wsId) throws Exception {
        log.info("workstation unmute, wsId: " + wsId);
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set mute_age = 0 where ws_id = " + wsId;
        db.execSql(sql);
    }


}
