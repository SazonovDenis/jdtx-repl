package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Предрелизные проверки - все должны проходить
 */
public class All_BaseRepl_Test extends AppTestCase {

    // Для возможности делать надежный disconnectAllForce именно для экземпляра теста,
    // иначе БД не отсвобождается
    DbPrepareEtalon_Test test;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        test = null;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        test.disconnectAllForce();
    }

    @Test
    // Прогон базового сценария репликации, полная двусторонняя репликация
    public void test_baseReplication() throws Exception {
        JdxReplWsSrv_Test test = new JdxReplWsSrv_Test();
        this.test = test;
        test.setUp();
        test.test_baseReplication();
    }

    @Test
    // Прогон базового сценария репликации, полная двусторонняя репликация
    // С фильтрами
    public void test_baseReplication_filter() throws Exception {
        JdxReplWsSrv_Test test = new JdxReplWsSrv_Test();
        this.test = test;
        test.setUp();
        test.test_baseReplication_filter();
    }

    @Test
    // Прогон сценария репликации: добавление рабочей станции после того, как остальные уже поработали некоторое время
    public void test_addWs() throws Exception {
        JdxReplWsSrv_AddWs_Test test = new JdxReplWsSrv_AddWs_Test();
        this.test = test;
        test.setUp();
        test.test_addWs();
    }

    @Test
    // Прогон сценария репликации: добавление рабочей станции после того, как остальные уже поработали некоторое время
    // С фильтрами
    public void test_addWs_filter() throws Exception {
        JdxReplWsSrv_AddWs_Test test = new JdxReplWsSrv_AddWs_Test();
        this.test = test;
        test.setUp();
        test.test_addWs_filter();
    }

    @Test
    public void test_Restore_06_Run() throws Exception {
        JdxReplWsSrv_Verdb_Test test = new JdxReplWsSrv_Verdb_Test();
        this.test = test;
        test.setUp();
        test.test_Restore_06_Run();
    }

    @Test
    public void test_allSetUp_CascadeDel() throws Exception {
        JdxReplWsSrv_DeleteCascade_Test test = new JdxReplWsSrv_DeleteCascade_Test();
        this.test = test;
        test.setUp();
        test.test_allSetUp_CascadeDel();
    }

    @Test
    public void test_failedInsertUpdate() throws Exception {
        JdxReplWsSrv_FailedInsertUpdate_Test test = new JdxReplWsSrv_FailedInsertUpdate_Test();
        this.test = test;
        test.setUp();
        test.test_failedInsertUpdate();
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
    public void test_ModifyDbStruct() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        this.test = test;
        test.setUp();
        test.test_ModifyDbStruct();
    }

}
