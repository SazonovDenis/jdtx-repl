package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import jandcode.web.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtAuditApplyer_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_applyReplica() throws Exception {
        //String zipFileName = "D:/t/000007590.zip";
        String zipFileName = "../_test-data/000000001.zip";
        // Загружаем правила публикации
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/pub.json"));
        JSONObject cfgIn = (JSONObject) cfg.get("full");
        IPublication publication = new Publication();
        publication.loadRules(cfgIn, struct2);

        // Реплики
        IReplica replica = new ReplicaFile();
        replica.setFile(new File(zipFileName));

        // Распакуем XML-файл из Zip-архива
        InputStream inputStream = UtRepl.getReplicaInputStream(replica);

        //
        JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

        // Применяем реплики
        UtAuditApplyer utaa = new UtAuditApplyer(db2, struct2);
        utaa.applyReplica(replicaReader, publication, false, 2, 0);
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


}