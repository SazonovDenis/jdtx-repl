package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.que.*;
import org.apache.commons.logging.*;

public class Update_008_009_que_out_no implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_008_009_que_out_no");

    @Override
    public void exec(Db db) throws Exception {
        JdxQuePersonal que = new JdxQuePersonal(db, UtQue.QUE_OUT, -1);
        //
        long age = que.getMaxAge();
        //
        que.setMaxNo(age);
    }

}
