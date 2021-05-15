package jdtx.repl.main.api;

import org.junit.*;

public class PkGeneratorService_Test extends ReplDatabaseStruct_Test {

    @Test
    public void testSvc() throws Exception {
        PkGeneratorService svc = app.service(PkGeneratorService.class);
        System.out.println("svc: " + svc);

        IPkGenerator generator = svc.createGenerator(db, struct);
        System.out.println("generator: " + generator);
        
        System.out.println("Lic.generator.name: " + generator.getGeneratorName("Lic"));
        System.out.println("Lic.generator.value: " + generator.getMaxPk("Lic"));
    }

}
