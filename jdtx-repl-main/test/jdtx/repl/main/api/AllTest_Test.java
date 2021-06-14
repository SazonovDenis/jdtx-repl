package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Предрелизные проверки - все должны проходить
 */
public class AllTest_Test extends AppTestCase {

    @Test
    public void test_0() throws Exception {
        JdxReplWsSrv_Test test5 = new JdxReplWsSrv_Test();
        test5.setUp();
        test5.test_allSetUp_TestAll();
        //
        test5 = new JdxReplWsSrv_Test();
        test5.setUp();
        test5.test_allSetUp_TestAll_filter();

        //
        JdxDeleteCascade_Test test0 = new JdxDeleteCascade_Test();
        test0.setUp();
        test0.test_allSetUp_CascadeDel();

        //
        JdxReplWsSrv_Verdb_Test test1 = new JdxReplWsSrv_Verdb_Test();
        test1.setUp();
        test1.test_Restore_06_Run();

        //
        JdxReplWsSrv_AddWs_Test test2 = new JdxReplWsSrv_AddWs_Test();
        test2.setUp();
        test2.test_all();
        test2 = new JdxReplWsSrv_AddWs_Test();
        test2.setUp();
        test2.test_allSetUp_TestAll_filter();

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
    }

}
