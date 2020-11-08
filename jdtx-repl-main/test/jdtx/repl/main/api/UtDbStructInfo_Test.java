package jdtx.repl.main.api;

import org.junit.*;

public class UtDbStructInfo_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test() throws Exception {
        UtDbStructInfo utDbStructInfo = new UtDbStructInfo(db, struct);
        String databaseInfo = utDbStructInfo.getDatabaseInfo();
        System.out.println("databaseInfo: " + databaseInfo);
    }

}
