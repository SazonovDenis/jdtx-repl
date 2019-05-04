package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import org.apache.commons.logging.*;

/**
 * Сервер: отметка состояния рабочих станций: MUTE и UMNMUTE.
 */
public class JdxMuteManagerSrv {

    private Db db;

    protected static Log log = LogFactory.getLog("jdtx");

    public JdxMuteManagerSrv(Db db) {
        this.db = db;
    }


    public void setMuteDone(long wsId, long muteAge) throws Exception {
        log.info("workstation mute done, wsId: " + wsId);
        //
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set mute_age = " + muteAge + " where ws_id = " + wsId;
        db.execSql(sql);
    }

    public void setUnmuteDone(long wsId) throws Exception {
        log.info("workstation unmute, wsId: " + wsId);
        //
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set mute_age = 0 where ws_id = " + wsId;
        db.execSql(sql);
    }


}