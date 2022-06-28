package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Проверка восстановления репликации рабочей станции при восстановлении базы/папок из бэкапа.
 * Врапер для запуска всех тестов jdtx.repl.main.api.JdxReplWsSrv_RestoreWs_***_test.
 */
public class All_RestoreWsAfterDbRestore_Test extends AppTestCase {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Подготовка репликационных баз и приведение их в нужное состояние
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.doNolmalLifeBromBackup = false;
        test.setUp();
        test.doSetUp_doNolmalLife_BeforeFail();
    }

    @Test
    public void test_Db1() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.setUp();
        test.doNolmalLifeBromBackup = true;
        test.test_Db1();
        test.disconnectAllForce();
    }

    @Test
    public void test_Db2() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.setUp();
        test.doNolmalLifeBromBackup = true;
        test.test_Db2();
        test.disconnectAllForce();
    }

    @Test
    public void test_Dir1() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.setUp();
        test.doNolmalLifeBromBackup = true;
        test.test_Dir1();
        test.disconnectAllForce();
    }

    @Test
    public void test_DirClean() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.setUp();
        test.doNolmalLifeBromBackup = true;
        test.test_DirClean();
        test.disconnectAllForce();
    }

    @Test
    public void test_Db1_Dir2() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.setUp();
        test.doNolmalLifeBromBackup = true;
        test.test_Db1_Dir2();
        test.disconnectAllForce();
    }

    @Test
    public void test_Db2_Dir1() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.setUp();
        test.doNolmalLifeBromBackup = true;
        test.test_Db2_Dir1();
        test.disconnectAllForce();
    }

    @Test
    public void test_Db1_DirClean() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.setUp();
        test.doNolmalLifeBromBackup = true;
        test.test_Db1_DirClean();
        test.disconnectAllForce();
    }

    @Test
    public void test_DirDB_srv() throws Exception {
        JdxReplWsSrv_RestoreWs_FromSrv_Test test = new JdxReplWsSrv_RestoreWs_FromSrv_Test();
        test.setUp();
        test.test_DirDB_srv();
        test.disconnectAllForce();
    }

    @Test
    public void test_All_filter() throws Exception {
        JdxReplWsSrv_RestoreWs_FromSrv_Test test = new JdxReplWsSrv_RestoreWs_FromSrv_Test();
        test.setUp();
        test.test_All_filter();
        test.disconnectAllForce();
    }

}
