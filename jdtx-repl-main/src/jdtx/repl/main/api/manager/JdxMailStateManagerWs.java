package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;

/**
 * Состояние почты: отметка, насколько отправлена почта - рабочая станция.
 * Реализация для рабочей станции отмечает только свою очередь out.
 */
public class JdxMailStateManagerWs implements IJdxMailStateManager {

    private Db db;

    public JdxMailStateManagerWs(Db db) {
        this.db = db;
    }


    /**
     * @return Номер реплики, до которого отправлена собственная почта рабочей станции
     */
    public long getMailSendDone() throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("mail_send_done");
    }

    public void setMailSendDone(long sendDone) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE set mail_send_done = " + sendDone;
        db.execSql(sql);
    }


}
