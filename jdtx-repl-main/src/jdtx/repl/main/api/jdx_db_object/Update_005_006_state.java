package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import org.apache.commons.logging.*;

public class Update_005_006_state implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_006_007");

    @Override
    public void exec(Db db) throws Exception {
        DbUtils dbu = new DbUtils(db);

        //
        //long que_out_no_que = dbu.loadSqlRec("select max(id) id from Z_Z_que_out").getValueLong("id");
        long que_in_no_que = dbu.loadSqlRec("select max(id) id from Z_Z_que_in").getValueLong("id");
        long que_common_no_que = dbu.loadSqlRec("select max(id) id from Z_Z_que_common").getValueLong("id");

        //
        //long que_out_no_state = dbu.loadSqlRec("select que_out_no from Z_Z_STATE").getValueLong("que_out_no");
        long que_in_no_state = dbu.loadSqlRec("select que_in_no from Z_Z_STATE").getValueLong("que_in_no");
        long que_common_no_state = dbu.loadSqlRec("select que_common_no from Z_Z_STATE").getValueLong("que_common_no");

        //
        //log.info("que_out_no_que: " + que_out_no_que + ", que_in_no_que: " + que_in_no_que + ", que_common_no " + que_common_no_que);
        //log.info("que_out_no_state: " + que_out_no_state + ", que_in_no_state: " + que_in_no_state + ", que_common_no_state " + que_common_no_state);
        log.info("que_in_no_que: " + que_in_no_que + ", que_common_no " + que_common_no_que);
        log.info("que_in_no_state: " + que_in_no_state + ", que_common_no_state " + que_common_no_state);

        //
        //boolean hasDifference = (que_out_no_state != que_out_no_que || que_in_no_state != que_in_no_que || que_common_no_state != que_common_no_que);
        boolean hasDifference = (que_in_no_state != que_in_no_que || que_common_no_state != que_common_no_que);

        //
        if (hasDifference) {
            //if (que_out_no_state != 0 || que_in_no_state != 0 || que_common_no_state != 0) {
            if (que_in_no_state != 0 || que_common_no_state != 0) {
                throw new XError("Not zero counters");
            }

            //
            dbu.updateRec("Z_Z_state", UtCnv.toMap(
                    "id", 1,
                    //"que_out_no_que", que_out_no_que,
                    "que_in_no_que", que_in_no_que,
                    "que_common_no", que_common_no_que
            ));
        }  else {
            log.info("No difference");
        }
    }

}
