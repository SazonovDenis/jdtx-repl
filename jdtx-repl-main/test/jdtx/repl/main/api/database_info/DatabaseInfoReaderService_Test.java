package jdtx.repl.main.api.database_info;

import jdtx.repl.main.api.*;
import org.junit.*;

public class DatabaseInfoReaderService_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void testSvc() throws Exception {
        DatabaseInfoReaderService svc = app.service(DatabaseInfoReaderService.class);
        System.out.println("svc: " + svc);

        IDatabaseInfoReader databaseInfoReader = svc.createDatabaseInfoReader(db, struct);
        System.out.println("databaseInfoReader: " + databaseInfoReader);
        System.out.println("readDbVersion: " + databaseInfoReader.readDatabaseVersion());
    }

}
