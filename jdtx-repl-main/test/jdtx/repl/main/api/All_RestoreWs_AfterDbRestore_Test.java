package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Предрелизные проверки - должны проходить все тесты.
 * Проверка восстановления репликации рабочей станции при восстановлении базы/папок из бэкапа.
 * Врапер для запуска тестов jdtx.repl.main.api.JdxReplWsSrv_RestoreWs_***_test.
 */
public class All_RestoreWs_AfterDbRestore_Test extends AppTestCase {

    // Для возможности делать надежный disconnectAllForce именно для экземпляра теста,
    // иначе БД не отсвобождается
    DbPrepareEtalon_Test test;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Подготовка репликационных баз и приведение их в нужное состояние
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        test.doNolmalLifeBromBackup = false;
        test.setUp();
        test.doSetUp_doNolmalLife_BeforeFail();
        test.disconnectAllForce();
    }

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
    public void test_Db1() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        this.test = test;
        test.testName = testName;
        test.doNolmalLifeBromBackup = true;
        test.setUp();
        test.test_Db1();
    }

    @Test
    public void test_Db2() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        this.test = test;
        test.testName = testName;
        test.doNolmalLifeBromBackup = true;
        test.setUp();
        test.test_Db2();
    }

    @Test
    public void test_Dir1() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        this.test = test;
        test.testName = testName;
        test.doNolmalLifeBromBackup = true;
        test.setUp();
        test.test_Dir1();
    }

    @Test
    public void test_DirClean() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        this.test = test;
        test.testName = testName;
        test.doNolmalLifeBromBackup = true;
        test.setUp();
        test.test_DirClean();
    }

    @Test
    public void test_Db1_Dir2() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        this.test = test;
        test.testName = testName;
        test.doNolmalLifeBromBackup = true;
        test.setUp();
        test.test_Db1_Dir2();
    }

    @Test
    public void test_Db2_Dir1() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        this.test = test;
        test.testName = testName;
        test.doNolmalLifeBromBackup = true;
        test.setUp();
        test.test_Db2_Dir1();
    }

    @Test
    public void test_Db1_DirClean() throws Exception {
        JdxReplWsSrv_RestoreWs_DbRestore_test test = new JdxReplWsSrv_RestoreWs_DbRestore_test();
        this.test = test;
        test.testName = testName;
        test.doNolmalLifeBromBackup = true;
        test.setUp();
        test.test_Db1_DirClean();
    }

}