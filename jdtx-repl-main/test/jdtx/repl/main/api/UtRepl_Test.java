package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.test.*;
import org.junit.*;

/**
 *
 */
public class UtRepl_Test extends DbmTestCase {


    @Test
    public void test_db() throws Exception {
        DataStore st = dbm.getDb().loadSql("select id, orgName from dbInfo");
        dbm.outTable(st);
    }

    @Test
    public void test_createReplication() throws Exception {
        UtRepl utr = new UtRepl(dbm.getUt());
        utr.createReplication();
    }

    @Test
    public void test_dropReplication() throws Exception {
        UtRepl utr = new UtRepl(dbm.getUt());
        utr.dropReplication();
    }


}
