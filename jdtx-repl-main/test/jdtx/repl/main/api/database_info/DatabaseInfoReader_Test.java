package jdtx.repl.main.api.database_info;

import jdtx.repl.main.api.*;
import org.junit.*;

public class DatabaseInfoReader_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_PS() throws Exception {
        IDatabaseInfoReader utDbStructInfo = new DatabaseInfoReader_PS(db, struct);
        String databaseInfo = utDbStructInfo.readDatabaseVersion();
        System.out.println("databaseInfo: " + databaseInfo);
    }

}
