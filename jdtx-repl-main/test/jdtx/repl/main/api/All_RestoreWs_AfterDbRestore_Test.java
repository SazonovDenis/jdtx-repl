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

    // Если нужно несколько раз позапускать один метод из этого класа, то чтобы сэкономить время на подготовке бэкапа,
    // можно установить один раз true (чтобы первый раз сделался бэкап), а потом можно установить false -
    // тогда перед запуском метода восстановление из бэкапа будет запускаться, а долгая подготовка бэкапа - нет.
    static boolean isClassTestMode = true;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if (isClassTestMode) {
            // Подготовка репликационных баз и приведение их в нужное состояние
            System.out.println("-------------");
            System.out.println("Делаем doBackupNolmalLife");
            System.out.println("-------------");

            //
            JdxReplWsSrv_RestoreWs_DbRestore_test testForBackup = new JdxReplWsSrv_RestoreWs_DbRestore_test();
            testForBackup.doNolmalLifeBromBackup = false;
            testForBackup.setUp();
            testForBackup.doSetUp_doNolmalLife_BeforeFail();
            testForBackup.disconnectAllForce();

            //
            System.out.println("-------------");
            System.out.println("doBackupNolmalLife - ok");
            System.out.println("-------------");
        }
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
