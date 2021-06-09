package jdtx.repl.main.api.manager;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
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
     * Отметка состояния станции: MUTE.
     */
    public void setMuteDone(long wsId, long muteAge) throws Exception {
        log.info("workstation mute done, wsId: " + wsId);
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set mute_age = " + muteAge + " where ws_id = " + wsId;
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
