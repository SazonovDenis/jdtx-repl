package jdtx.repl.main.api.jdx_db_object;

import jandcode.app.test.*;
import org.junit.*;

public class SqlScriptExecutorService_Test extends AppTestCase {

    @Test
    public void testSvc() {
        SqlScriptExecutorService svc = app.service(SqlScriptExecutorService.class);
        ISqlScriptExecutor o1 = svc.createByName("Update_005_006");
        System.out.println("Script: " + o1);
        ISqlScriptExecutor o2 = svc.createByName("Update_None");
        System.out.println("Script: " + o2);
    }

}
