package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.que.*;
import org.junit.*;

import java.io.*;

public class Update_011_012_fill_age_Test extends DbmTestCase {

    @Test
    public void test() throws Exception {
        logOn();
        //
        Db db = dbm.getDb();
        System.out.println(db.getDbSource().getDatabase());
        //
        SqlScriptExecutorService svc = app.service(SqlScriptExecutorService.class);
        ISqlScriptExecutor script = svc.createByName("Update_011_012_fill_age");
        script.exec(db);
    }

}