package jdtx.repl.main.api.ref_manager;

import jdtx.repl.main.api.*;
import org.junit.*;

public class RefManagerDecodeService_Test extends Database_Test {

    @Test
    public void testSvc() throws Exception {
        RefManagerService svc = app.service(RefManagerService.class);
        System.out.println("svc: " + svc.getClass().getName());
    }

}
