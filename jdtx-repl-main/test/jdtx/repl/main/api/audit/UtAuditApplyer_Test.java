package jdtx.repl.main.api.audit;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class UtAuditApplyer_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_applyReplicaFile() throws Exception {
        // --- CashEncashReason - чистим
        db2.execSql("delete from CashEncash where id <> 0");
        db2.execSql("delete from CashEncashReason where id <> 0");

        // --- CashEncashReason - было
        DataStore st0 = db2.loadSql("select * from CashEncashReason");
        UtData.outTable(st0);

        // --- Наполняем CashEncashReason - из реплики
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();

        String replicaFileName = "../_test-data/_test-data_ws2/ws_002/queIn/000000086.zip";
        File replicaFile = new File(replicaFileName);

        ws2.useReplicaFile(replicaFile);

        // --- CashEncashReason - стало
        DataStore st1 = db2.loadSql("select * from CashEncashReason");
        UtData.outTable(st1);
    }

    @Test
    public void test_applyReplicaFile_1() throws Exception {
        // ---
        DataStore st0 = db2.loadSql("select * from lic where rnn = '890415302076'");
        UtData.outTable(st0);

        // ---
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();

        String replicaFileName = "D:/t/esilMK/003/000009370.zip";
        File replicaFile = new File(replicaFileName);

        ws2.useReplicaFile(replicaFile);

        // ---
        DataStore st1 = db2.loadSql("select * from lic where rnn = '890415302076'");
        UtData.outTable(st1);
    }


}
