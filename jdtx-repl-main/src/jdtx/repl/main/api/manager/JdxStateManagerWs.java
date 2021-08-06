package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;

/**
 * Состояние задач рабочей станции: отметки в БД, насколько отработаны очереди и прочее.
 * todo: А не кажется ли вам, что методы setQueOutNoDone и setQueNoDone друг друга дублируют?????
 */
public class JdxStateManagerWs {

    private Db db;

    public JdxStateManagerWs(Db db) {
        this.db = db;
    }


    /**
     * @return Номер (ранее - возраст аудита), до которого сформирована исходящая очередь
     */
    public long getQueOutNoDone() throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_out_no_done");
    }

    /**
     * Задает номер (ранее - возраст аудита), до которого сформирована исходящая очередь
     * (заполняется при выкладывании реплики в исходящую очередь).
     */
    public void setQueOutNoDone(long queOutAgeDone) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE set que_out_no_done = " + queOutAgeDone;
        db.execSql(sql);
    }


    /**
     * @return Номер реплики, до которого обработана (применена и т.п.) очередь (queIn, queIn001 и т.п.)
     */
    public void setQueNoDone(String queName, long queInNoDone) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE set que_" + queName + "_no_done = " + queInNoDone;
        db.execSql(sql);
    }

    public long getQueNoDone(String queName) throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("que_" + queName + "_no_done");
    }

}
