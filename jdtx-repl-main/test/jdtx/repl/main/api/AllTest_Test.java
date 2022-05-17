package jdtx.repl.main.api;

import jandcode.app.test.*;
import jdtx.repl.main.api.rec_merge.*;
import org.junit.*;

/**
 * Предрелизные проверки - все должны проходить
 */
public class AllTest_Test extends AppTestCase {

    @Test
    public void test_0() throws Exception {
        // Прогон базового сценария репликации, полная двусторонняя репликация
        JdxReplWsSrv_Test test0 = new JdxReplWsSrv_Test();
        test0.setUp();
        test0.test_allSetUp_TestAll();
        // С фильтрами
        test0 = new JdxReplWsSrv_Test();
        test0.setUp();
        test0.test_allSetUp_TestAll_filter();

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

        // Проверка восстановления репликации рабочей станции при полной потере базы и репликационных каталогов, по данным с сервера.
        JdxReplWsSrv_RestoreWs_FromSrv_Test test6 = new JdxReplWsSrv_RestoreWs_FromSrv_Test();
        test6.setUp();
        test6.test_DirDB_srv();
        // С фильтрами
        test6 = new JdxReplWsSrv_RestoreWs_FromSrv_Test();
        test6.setUp();
        test6.test_All_filter();

        //  Проверка восстановления репликации рабочей станции при восстановлении базы/папок из бэкапа.
        JdxReplWsSrv_RestoreWs_DbRestore_test test7 = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test7.setUp();
        test7.test_Db1();
        //
        test7 = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test7.setUp();
        test7.test_Dir1();
        //
        test7 = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test7.setUp();
        test7.test_DirClean();
        //
        test7 = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test7.setUp();
        test7.test_Db1_Dir2();
        //
        test7 = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test7.setUp();
        test7.test_Db2_Dir1();
        //
        test7 = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test7.setUp();
        test7.test_Db1_DirClean();

        //
        JdxReplWsSrv_DeleteCascade_Test test5 = new JdxReplWsSrv_DeleteCascade_Test();
        test5.setUp();
        test5.test_allSetUp_CascadeDel();

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

        // Все тесты JdxRecMerge_Test
        JdxRecMerge_Test testMerge = new JdxRecMerge_Test();
        // Выполнить все тесты в классе testMerge

        // Создание репликации и удаление дубликтов, которые появились после превичного слияния
        JdxReplWsSrv_Merge_Test testMergeWsSrv = new JdxReplWsSrv_Merge_Test();
        testMergeWsSrv.setUp();
        testMergeWsSrv.test_SetUp_Merge();
        // Создиние дубликатов на работающей системе и их удаление
        testMergeWsSrv.test_LicDoc_MergeCommand();
        testMergeWsSrv.test_CommentTip_jc();
    }

}
