package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

/**
 * Предрелизные проверки - должны проходить все тесты
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        All_BaseRepl_Test.class,
        All_Mailer_Test.class,
        All_MergeCore_Test.class,
        All_MergeRepl_Test.class,
        All_RestoreWsAfterDbRestore_Test.class,
})
public class All_Test extends AppTestCase {

}
