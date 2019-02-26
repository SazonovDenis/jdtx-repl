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
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.sys_table_prefix + "state_ws");
        } else {
            return rec.getValueLong("que_in_age_done");
        }
    }

    public void setWsQueInAgeDone(long wsId, long queInAgeDone) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set que_in_age_done = " + queInAgeDone + " where ws_id = " + wsId;
        db.execSql(sql);
    }


    /**
     * @return Номер реплики, до которого обработана общая очередь
     * при тиражировании реплик
     */
    public long getCommonQueNoDone(long wsId) throws Exception {
        String sql = "select * from " + JdxUtils.sys_table_prefix + "state_ws where ws_id = " + wsId;
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Не найдена запись для ws_id [" + wsId + "] в " + JdxUtils.sys_table_prefix + "state_ws");
        } else {
            long no = rec.getValueLong("que_common_no_done");
            // Номер в очередях (в отличие от возраста) начинается от 1,
            // но возраст в очередях может начаться с 0
            if (no == -1) {
                no = 0;
            }
            return no;
        }
    }

    public void setCommonQueNoDone(long wsId, long queCommonNoDone) throws Exception {
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set que_common_no_done = " + queCommonNoDone + " where ws_id = " + wsId;
        db.execSql(sql);
    }


}
