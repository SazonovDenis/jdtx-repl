package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;

/**
 * Состояние почты: отметка, насколько отправлена почта.
 */
public class JdxStateManagerMail implements IJdxStateManagerMail {

    private Db db;

    public JdxStateManagerMail(Db db) {
        this.db = db;
    }


    /**
     * @return Возраст реплики, до которого отправлена собственная почта рабочей станции
     */
    public long getMailSendDone() throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "state";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("mail_send_done");
    }

    public void setMailSendDone(long sendDone) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "state set mail_send_done = " + sendDone;
        db.execSql(sql);
    }


}
