package jdtx.repl.main.api.decoder;

import jdtx.repl.main.api.*;
import org.junit.*;

public class RefDecoderService_Test extends Database_Test {

    @Test
    public void testSvc() throws Exception {
        JdxRefDecoderService svc = app.service(JdxRefDecoderService.class);
        System.out.println("svc: " + svc);

        IRefDecoder refDecoder = svc.createRefDecoder(db1, 999);
        System.out.println("refDecoder: " + refDecoder);
    }

}
