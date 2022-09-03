package jdtx.repl.main.api.decoder;

import jdtx.repl.main.api.*;
import org.junit.*;

public class RefManagerDecodeService_Test extends Database_Test {

    @Test
    public void testSvc() throws Exception {
        RefManagerService svc = app.service(RefManagerService.class);
        System.out.println("svc: " + svc.getClass().getName());

        IRefManager refDecoder = svc.createRefManager(db1, 999);
        System.out.println("refDecoder: " + refDecoder.getClass().getName());
    }

}
