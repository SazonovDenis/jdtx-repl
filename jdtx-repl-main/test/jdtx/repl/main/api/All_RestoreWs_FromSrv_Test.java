package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Проверка восстановления репликации рабочей станции
 * по данным с сервера.
 */
public class All_RestoreWs_FromSrv_Test extends AppTestCase {

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
    public void test_DirDB_srv() throws Exception {
        JdxReplWsSrv_RestoreWs_FromSrv_Test test = new JdxReplWsSrv_RestoreWs_FromSrv_Test();
        this.test = test;
        test.testName = testName;
        test.setUp();
        test.test_DirDB_srv();
    }

    @Test
    public void test_DirDB_srv_filter() throws Exception {
        JdxReplWsSrv_RestoreWs_FromSrv_Test test = new JdxReplWsSrv_RestoreWs_FromSrv_Test();
        this.test = test;
        test.testName = testName;
        test.setUp();
        test.test_DirDB_srv_filter();
    }

}
