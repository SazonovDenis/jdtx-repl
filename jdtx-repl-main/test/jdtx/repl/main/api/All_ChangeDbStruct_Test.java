package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Предрелизные проверки - должны проходить все тесты.
 * Смена версии БД.
 * Врапер для запуска тестов jdtx.repl.main.api.JdxReplWsSrv_ChangeDbStruct_Test.
 */
public class All_ChangeDbStruct_Test extends AppTestCase {

    // Для возможности делать надежный disconnectAllForce именно для экземпляра теста,
    // иначе БД не отсвобождается
    DbPrepareEtalon_Test test;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Подготовка репликационных баз и приведение их в нужное состояние
        JdxReplWsSrv_Test test = new JdxReplWsSrv_Test();
        test.setUp();
        test.test_baseReplication();
        //
        JdxReplWsSrv_RestoreWs_DbRestore_test testForBackup = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        testForBackup.setUp();
        testForBackup.doBackupNolmalLife();
        //
        test.disconnectAllForce();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        test = null;
        //
        JdxReplWsSrv_RestoreWs_DbRestore_test testForBackup = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        testForBackup.setUp();
        testForBackup.doRestoreFromNolmalLife();
        testForBackup.disconnectAllForce();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        test.disconnectAllForce();
    }

    @Test
    public void test_Mute_Unmute() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        this.test = test;
        test.setUp();
        test.test_Mute_Unmute();
    }

    @Test
    public void test_No_ApplyReplicas() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        this.test = test;
        test.setUp();
        test.test_No_ApplyReplicas();
    }

    @Test
    public void test_No_HandleSelfAudit() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        this.test = test;
        test.setUp();
        test.test_No_HandleSelfAudit();
    }

    @Test
    public void test_auditAfterInsDel() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        this.test = test;
        test.setUp();
        test.test_auditAfterInsDel();
    }

    @Test
    public void test_ModifyDbStruct() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        this.test = test;
        test.setUp();
        test.test_ModifyDbStruct();
    }

    @Test
    public void test_modifyDbStruct_triple() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        this.test = test;
        test.setUp();
        test.test_modifyDbStruct_triple();
    }


}
