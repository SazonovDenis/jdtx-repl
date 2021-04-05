package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import org.junit.*;

public class Update_005_006_state_Test extends DbmTestCase {

    @Test
    public void testSvc() throws Exception {
        logOn();
        //
        Db db = dbm.getDb();
        System.out.println(db.getDbSource().getDatabase());
        //
        SqlScriptExecutorService svc = app.service(SqlScriptExecutorService.class);
        ISqlScriptExecutor script = svc.createByName("Update_005_006_state");
        script.exec(db);
    }

}
