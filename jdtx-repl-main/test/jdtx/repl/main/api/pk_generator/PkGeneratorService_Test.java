package jdtx.repl.main.api.pk_generator;

import jdtx.repl.main.api.*;
import org.junit.*;

public class PkGeneratorService_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void testSvc() throws Exception {
        PkGeneratorService svc = app.service(PkGeneratorService.class);
        System.out.println("svc: " + svc);

        IPkGenerator generator = svc.createGenerator(db2, struct2);
        System.out.println("generator: " + generator);
        
        System.out.println("Lic.generator.name: " + generator.getGeneratorName("Lic"));
        System.out.println("Lic.generator.value: " + generator.getMaxPk("Lic"));
    }

}
