package jdtx.repl.main.api.jdx_db_object;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.jdx_db_object.*;
import org.junit.*;

public class UtDbObjectManager_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_checkReplVerDb() throws Exception {
        UtDbObjectManager objectManager = new UtDbObjectManager(db, struct);
        objectManager.checkReplVerDb();
    }

}
