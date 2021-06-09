package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import org.apache.commons.logging.*;

/**
 * Рабочая станция: отметка собственного состояния: MUTE и UMNMUTE.
 */
public class JdxMuteManagerWs {

    private Db db;

    protected static Log log = LogFactory.getLog("jdtx.MuteManagerWs");

    static long STATE_MUTE_OFF = 0;
    static long STATE_MUTE = 1;


    public JdxMuteManagerWs(Db db) {
        this.db = db;
    }


    public void muteWorkstation() throws Exception {
        log.info("mute workstation");
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE set mute = " + STATE_MUTE;
        db.execSql(sql);
    }

    public void unmuteWorkstation() throws Exception {
        log.info("unmute workstation");
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE set mute = " + STATE_MUTE_OFF;
        db.execSql(sql);
    }

    public boolean isMute() throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("mute") == STATE_MUTE;
    }


}
