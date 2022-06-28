package jdtx.repl.main.api;

import jandcode.app.test.*;
import jdtx.repl.main.api.rec_merge.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

/**
 * Предрелизные проверки - должны проходить все тесты jdtx.repl.main.api.rec_merge.***_Test
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        GroupStrategy_Test.class,
        JdxRecMerge_Test.class,
        JdxRecRelocator_Test.class,
        JdxRecRemover_Test.class,
})
public class All_MergeCore_Test extends AppTestCase {

    @BeforeClass
    public static void test_baseReplication() throws Exception {
        // Подготовка репликационных баз и приведение их в нужное состояние
        JdxReplWsSrv_Test test = new JdxReplWsSrv_Test();
        test.setUp();
        test.test_baseReplication();
        test.disconnectAllForce();
    }

}
