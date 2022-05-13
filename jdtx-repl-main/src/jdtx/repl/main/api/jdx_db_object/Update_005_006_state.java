package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import org.apache.commons.logging.*;

public class Update_005_006_state implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_005_006_state");

    @Override
    public void exec(Db db) throws Exception {
        // ---
        // Новые поля в Z_Z_STATE
        // que_in_no
        // que_common_no
        // ---

        //
        DbUtils dbu = new DbUtils(db);

        // Ранее была другая методика вычисления размера очереди

        // Читаем текущий размер очереди queIn и queCommon
        long que_in_no_que = dbu.loadSqlRec("select max(id) id from Z_Z_QUE_IN").getValueLong("id");
        long que_common_no_que = dbu.loadSqlRec("select max(id) id from Z_Z_QUE_COMMON").getValueLong("id");

        //
        // Читаем текущие отметки размера очередей queOut, queIn и queCommon в Z_Z_STATE
        long que_in_no_state = dbu.loadSqlRec("select que_in_no from Z_Z_STATE").getValueLong("que_in_no");
        long que_common_no_state = dbu.loadSqlRec("select que_common_no from Z_Z_STATE").getValueLong("que_common_no");

        //
        log.info("que_in_no_que: " + que_in_no_que + ", que_common_no " + que_common_no_que);
        log.info("que_in_no_state: " + que_in_no_state + ", que_common_no_state " + que_common_no_state);

        //
        boolean hasDifference = (que_in_no_state != que_in_no_que || que_common_no_state != que_common_no_que);

        //
        if (hasDifference) {
            if (que_in_no_state != 0 || que_common_no_state != 0) {
                throw new XError("Not zero que counters in Z_Z_STATE");
            }

            //
            db.execSql("update Z_Z_state set que_in_no = " + que_in_no_que);
            db.execSql("update Z_Z_state set que_common_no = " + que_common_no_que);
        } else {
            log.info("No difference");
        }


        // ---
        // Новые поля в Z_Z_STATE не требуют инициализации т.к. исходно равны нулю
        // que_in001_no
        // que_in001_no_done
        // ---


        // ---
        // Новые поля в Z_Z_STATE_WS
        // que_out000_send_done
        // que_out000_no
        // ---

        //DataStore ds = db.loadSql("select * from Z_Z_STATE_WS");
        //UtData.outTable(ds);

        // Ранее que_common_dispatch_done всегда совпадал с возрастом рассылки,
        // т.к. que_common_dispatch_done отмечалась после рассылки на все филиалы
        db.execSql("update Z_Z_STATE_WS set que_out000_send_done = que_common_dispatch_done");
        log.info("update Z_Z_STATE_WS set que_out000_send_done");


        // Ранее очередь queCommon раздавалась напрямую на все филиалы
        db.execSql("update Z_Z_STATE_WS set que_out000_no = que_common_dispatch_done");
        log.info("update Z_Z_STATE_WS set que_out000_no");


        // ---
        // Новые поля в Z_Z_STATE_WS не требуют инициализации т.к. исходно равны нулю
        // que_out001_no
        // que_out001_send_done
        // ---
    }

}
