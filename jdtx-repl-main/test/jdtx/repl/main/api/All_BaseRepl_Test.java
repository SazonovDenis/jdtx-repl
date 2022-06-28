package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Предрелизные проверки - все должны проходить
 */
public class All_BaseRepl_Test extends AppTestCase {

    @Test
    // Прогон базового сценария репликации, полная двусторонняя репликация
    public void test_baseReplication() throws Exception {
        JdxReplWsSrv_Test test = new JdxReplWsSrv_Test();
        test.setUp();
        test.test_baseReplication();
        test.disconnectAllForce();
    }

    @Test
    // Прогон базового сценария репликации, полная двусторонняя репликация
    // С фильтрами
    public void test_baseReplication_filter() throws Exception {
        JdxReplWsSrv_Test test = new JdxReplWsSrv_Test();
        test.setUp();
        test.test_baseReplication_filter();
        test.disconnectAllForce();
    }

    @Test
    // Прогон сценария репликации: добавление рабочей станции после того, как остальные уже поработали некоторое время
    public void test_addWs() throws Exception {
        JdxReplWsSrv_AddWs_Test test = new JdxReplWsSrv_AddWs_Test();
        test.setUp();
        test.test_addWs();
        test.disconnectAllForce();
    }

    @Test
    // Прогон сценария репликации: добавление рабочей станции после того, как остальные уже поработали некоторое время
    // С фильтрами
    public void test_addWs_filter() throws Exception {
        JdxReplWsSrv_AddWs_Test test = new JdxReplWsSrv_AddWs_Test();
        test.setUp();
        test.test_addWs_filter();
        test.disconnectAllForce();
    }

    @Test
    public void test_Restore_06_Run() throws Exception {
        JdxReplWsSrv_Verdb_Test test = new JdxReplWsSrv_Verdb_Test();
        test.setUp();
        test.test_Restore_06_Run();
        test.disconnectAllForce();
    }

    @Test
    public void test_allSetUp_CascadeDel() throws Exception {
        JdxReplWsSrv_DeleteCascade_Test test = new JdxReplWsSrv_DeleteCascade_Test();
        test.setUp();
        test.test_allSetUp_CascadeDel();
        test.disconnectAllForce();
    }

    @Test
    public void test_failedInsertUpdate() throws Exception {
        JdxReplWsSrv_FailedInsertUpdate_Test test = new JdxReplWsSrv_FailedInsertUpdate_Test();
        test.setUp();
        test.test_failedInsertUpdate();
        test.disconnectAllForce();
    }

    @Test
    public void test_Mute_Unmute() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        test.setUp();
        test.test_Mute_Unmute();
        test.disconnectAllForce();
    }

    @Test
    public void test_No_ApplyReplicas() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        test.setUp();
        test.test_No_ApplyReplicas();
        test.disconnectAllForce();
    }

    @Test
    public void test_No_HandleSelfAudit() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        test.setUp();
        test.test_No_HandleSelfAudit();
        test.disconnectAllForce();
    }

    @Test
    public void test_ModifyDbStruct() throws Exception {
        JdxReplWsSrv_ChangeDbStruct_Test test = new JdxReplWsSrv_ChangeDbStruct_Test();
        test.setUp();
        test.test_ModifyDbStruct();
        test.disconnectAllForce();
    }

}
