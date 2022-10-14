package jdtx.repl.main.api.audit;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.io.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;

public class AuditTest_Test extends AppTestCase {

    Db db_test;
    IJdxDbStruct db_test_struct;

    public void setUp() throws Exception {
        super.setUp();

        //
        UtLog.loadProperties("../_log.properties");
        logOn();

        //
        Model m = app.getApp().service(ModelService.class).getModel("db_test");
        //
        db_test = m.getDb();
        db_test.connect();

        //
        IJdxDbStructReader reader = new JdxDbStructReader();
        //
        reader.setDb(db_test);
        db_test_struct = reader.readDbStruct();


        // ---
        // Чтобы были
        UtFile.mkdirs("temp");
    }


    @Test
    public void test_140() throws Exception {
        logOn();

        // Загружаем правила публикации
        JSONObject cfg = UtRepl.loadAndValidateJsonFile("test/etalon/publication_full_152.json");
        IPublicationRuleStorage publication = PublicationRuleStorage.loadRules(cfg, db_test_struct, "in");

        // Формируем реплики
        long wsId = 2;
        long wsAuditAge = 140;
        UtAuditSelector auditSelector = new UtAuditSelector(db_test, db_test_struct);
        IReplica replica = auditSelector.createReplicaFromAudit(wsId, publication, wsAuditAge);
        // Переносим файл на постоянное место
        File actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getData(), actualFile);
        System.out.println(actualFile.getAbsolutePath());
    }

    @Test
    public void test_132_145() throws Exception {
        // Загружаем правила публикации
        JSONObject cfg = UtRepl.loadAndValidateJsonFile("test/etalon/publication_full_152.json");
        IPublicationRuleStorage publication = PublicationRuleStorage.loadRules(cfg, db_test_struct, "in");

        // Формируем реплики
        long wsId = 2;
        long wsAuditAge = 139;
        UtAuditSelector auditSelector = new UtAuditSelector(db_test, db_test_struct);
        IReplica replica = auditSelector.createReplicaFromAudit(wsId, publication, wsAuditAge);
        // Переносим файл на постоянное место
        File actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getData(), actualFile);
        System.out.println(actualFile.getAbsolutePath());

        //
        wsAuditAge = 140;
        replica = auditSelector.createReplicaFromAudit(wsId, publication, wsAuditAge);
        actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getData(), actualFile);
        System.out.println(actualFile.getAbsolutePath());

        //
        wsAuditAge = 141;
        replica = auditSelector.createReplicaFromAudit(wsId, publication, wsAuditAge);
        actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getData(), actualFile);
        System.out.println(actualFile.getAbsolutePath());

        //
        wsAuditAge = 145;
        replica = auditSelector.createReplicaFromAudit(wsId, publication, wsAuditAge);
        actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getData(), actualFile);
        System.out.println(actualFile.getAbsolutePath());
    }

}
