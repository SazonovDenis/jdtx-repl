package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import org.apache.commons.logging.*;

/**
 *
 */
public class Update_014_015_que_generators implements ISqlScriptExecutor {

    protected Log log = LogFactory.getLog("jdtx.Update_014_015_que_generators");

    @Override
    public void exec(Db db) throws Exception {
        //doIt(db, "srv_que_common");
        doIt(db, "srv_que_out000");
        doIt(db, "srv_que_out001");
    }

    public void doIt(Db db, String queName) throws Exception {
        DataStore st = db.loadSql("select max(id) as id from Z_Z_" + queName);
        long id = st.getCurRec().getValueLong("id");
        db.execSql("SET generator Z_Z_G_" + queName + " TO " + id);
    }


}