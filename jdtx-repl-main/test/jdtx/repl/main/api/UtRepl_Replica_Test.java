package jdtx.repl.main.api;

import org.junit.*;

/**
 * —оздание/удаление репликационных структур
 */
public class UtRepl_Replica_Test extends Repl_Test_Custom {


    @Test
    public void test_CreateReplica() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());
        IPublication publcation = new Publication();
        IReplica replica = utr.createReplica(publcation, 0, 100);
    }


}
