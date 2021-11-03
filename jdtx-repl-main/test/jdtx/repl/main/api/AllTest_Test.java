package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Предрелизные проверки - все должны проходить
 */
public class AllTest_Test extends AppTestCase {

    @Test
    public void test_0() throws Exception {
        // Прогон базового сценария репликации, полная двусторонняя репликация
        JdxReplWsSrv_Test test5 = new JdxReplWsSrv_Test();
        test5.setUp();
        test5.test_allSetUp_TestAll();
        // С фильтрами
        test5 = new JdxReplWsSrv_Test();
        test5.setUp();
        test5.test_allSetUp_TestAll_filter();

        // Прогон сценария репликации: добавление рабочей станции после того, как остальные уже поработали некоторое время
        JdxReplWsSrv_AddWs_Test test2 = new JdxReplWsSrv_AddWs_Test();
        test2.setUp();
        test2.test_all();
        // С фильтрами
        test2 = new JdxReplWsSrv_AddWs_Test();
        test2.setUp();
        test2.test_allSetUp_TestAll_filter();

        //
        JdxReplWsSrv_Verdb_Test test1 = new JdxReplWsSrv_Verdb_Test();
        test1.setUp();
        test1.test_Restore_06_Run();

        // Прогон сценария репликации: восстановление утраченной базы рабочей станции по данным с сервера
        JdxReplWsSrv_RestoreWsFromSrv_Test test6 = new JdxReplWsSrv_RestoreWsFromSrv_Test();
        test6.setUp();
        test6.test_All();
        // С фильтрами
        test6 = new JdxReplWsSrv_RestoreWsFromSrv_Test();
        test6.setUp();
        test6.test_All_filter();

        //
        JdxDeleteCascade_Test test0 = new JdxDeleteCascade_Test();
        test0.setUp();
        test0.test_allSetUp_CascadeDel();

        //
        JdxReplWsSrv_FailedInsertUpdate_Test test3 = new JdxReplWsSrv_FailedInsertUpdate_Test();
        test3.setUp();
        test3.test_all();

        //
        JdxReplWsSrv_ChangeDbStruct_Test test4 = new JdxReplWsSrv_ChangeDbStruct_Test();
        test4.setUp();
        test4.test_Mute_Unmute();
        //
        test4 = new JdxReplWsSrv_ChangeDbStruct_Test();
        test4.setUp();
        test4.test_No_ApplyReplicas();
        //
        test4 = new JdxReplWsSrv_ChangeDbStruct_Test();
        test4.setUp();
        test4.test_No_HandleSelfAudit();
        //
        test4 = new JdxReplWsSrv_ChangeDbStruct_Test();
        test4.setUp();
        test4.test_ModifyDbStruct();

        // Проверка ремонта базы при восстановлении из бэкапа
        DatabaseRestore_test test7 = new DatabaseRestore_test();
        test7.test_DatabaseRestore_step1();
        test7.test_DatabaseRestore_step2();
    }

}
