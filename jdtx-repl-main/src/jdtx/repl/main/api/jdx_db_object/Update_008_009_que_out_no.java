package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.que.*;
import org.apache.commons.logging.*;

public class Update_008_009_que_out_no implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_008_009_que_out_no");

    @Override
    public void exec(Db db) throws Exception {
        JdxQuePersonal que = new JdxQuePersonal(db, UtQue.QUE_OUT, -1);
        //
        long que_out_age = que.getMaxAge();
        long que_out_no = db.loadSql("select max(id) id from " + UtJdx.SYS_TABLE_PREFIX + "que_" + UtQue.getTableSuffix(UtQue.QUE_OUT)).getCurRec().getValueLong("id");
        //
        que.setMaxNo(que_out_no);
        //
        log.info("que_out_age: " + que_out_age + ", que_out_no: " + que_out_no);
    }

}
