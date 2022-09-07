package jdtx.repl.main.api.ref_manager;

import jandcode.app.test.*;
import org.junit.*;

public class RefManagerService_Test extends AppTestCase {

    @Test
    public void testSvc() throws Exception {
        RefManagerService svc = app.service(RefManagerService.class);
        System.out.println("svc: " + svc.getClass().getName());
    }

}
