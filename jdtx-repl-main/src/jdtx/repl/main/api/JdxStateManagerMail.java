package jdtx.repl.main.api;

import jandcode.dbm.data.DataRecord;
import jandcode.dbm.db.Db;
import jandcode.utils.error.XError;

/**
 * Состояние почты: отметка, насколько отправлена почта.
 */
public class JdxStateManagerMail {

    private Db db;

    public JdxStateManagerMail(Db db) {
        this.db = db;
    }


    /**
     * @return Возраст реплики, до которого отправлена почта для рабочей станции wsId
     */
/*
    public long getMailSendDoneForWs(long wsId) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.sys_table_prefix + "state_ws");
        } else {
            return rec.getValueLong("mail_send_done");
        }
    }

    public void setMailSendDoneForWs(long wsId, long mailSendDone) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set mail_send_done = " + mailSendDone + " where ws_id = " + wsId;
        db.execSql(sql);
    }
*/


    /**
     * @return Возраст реплики, до которого отправлена собственная почта рабочей станции
     */
    public long getMailSendDone() throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("mail_send_done");
    }

    public void setMailSendDone(long mailSendDone) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state set mail_send_done = " + mailSendDone;
        db.execSql(sql);
    }


}
