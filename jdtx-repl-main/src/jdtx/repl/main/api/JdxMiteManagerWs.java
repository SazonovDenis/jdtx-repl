package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import org.apache.commons.logging.*;

/**
 * Состояние рабочей станции: MITE и UMNMITE.
 */
public class JdxMiteManagerWs {

    private Db db;

    protected static Log log = LogFactory.getLog("jdtx");


    public JdxMiteManagerWs(Db db) {
        this.db = db;
    }


    public void miteWorkstation(long wsId) throws Exception {
        log.info("mite workstation, wsId: " + wsId);
        //
        String sql = "update " + JdxUtils.sys_table_prefix + "state set mute = 1 where id = " + wsId;
        db.execSql(sql);
    }

    public void unmuteWorkstation(long wsId) throws Exception {
        log.info("unmute workstation, wsId: " + wsId);
        //
        String sql = "update " + JdxUtils.sys_table_prefix + "state set mute = 0 where id = " + wsId;
        db.execSql(sql);
    }


}
