package jdtx.repl.main.api;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.web.*;
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
        //
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

        //
        UtRepl utRepl = new UtRepl(db_test, db_test_struct);

        // Загружаем правила публикации
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/publication_full_152.json"));
        IPublication publication = new Publication();
        publication.loadRules(cfg, db_test_struct);

        // Формируем реплики
        long wsId = 2;
        long wsAuditAge = 140;
        IReplica replica = utRepl.createReplicaFromAudit(wsId, publication, wsAuditAge);
        // Переносим файл на постоянное место
        File actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getFile(), actualFile);
        System.out.println(actualFile.getAbsolutePath());
    }

    @Test
    public void test_132_145() throws Exception {
        UtRepl utRepl = new UtRepl(db_test, db_test_struct);

        // Загружаем правила публикации
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/publication_full_152.json"));
        IPublication publication = new Publication();
        publication.loadRules(cfg, db_test_struct);

        // Формируем реплики
        long wsId = 2;
        long wsAuditAge = 139;
        IReplica replica = utRepl.createReplicaFromAudit(wsId, publication, wsAuditAge);
        // Переносим файл на постоянное место
        File actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getFile(), actualFile);
        System.out.println(actualFile.getAbsolutePath());

        //
        wsAuditAge = 140;
        replica = utRepl.createReplicaFromAudit(wsId, publication, wsAuditAge);
        actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getFile(), actualFile);
        System.out.println(actualFile.getAbsolutePath());

        //
        wsAuditAge = 141;
        replica = utRepl.createReplicaFromAudit(wsId, publication, wsAuditAge);
        actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getFile(), actualFile);
        System.out.println(actualFile.getAbsolutePath());

        //
        wsAuditAge = 145;
        replica = utRepl.createReplicaFromAudit(wsId, publication, wsAuditAge);
        actualFile = new File("../_test-data/~tmp_csv-" + wsAuditAge + ".zip");
        FileUtils.moveFile(replica.getFile(), actualFile);
        System.out.println(actualFile.getAbsolutePath());
    }

}
