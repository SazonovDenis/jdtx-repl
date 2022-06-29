package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Предрелизные проверки - должны проходить все тесты.
 * Удаление дубликтов, которые появились на сервере после подключения базы в систему репликации.
 * Врапер для запуска тестов jdtx.repl.main.api.JdxReplWsSrv_Merge_Test.
 */
public class All_MergeRepl_Test extends AppTestCase {

    @Test
    // Создание репликации и удаление дубликтов, которые появились после подключения базы в систему репликации
    public void test_SetUp_Merge() throws Exception {
        JdxReplWsSrv_Merge_Test test = new JdxReplWsSrv_Merge_Test();
        test.setUp();
        test.test_SetUp_Merge();
    }

    @Test
    // Создиние дубликатов на работающей системе и их удаление
    public void test_LicDoc_MergeCommand() throws Exception {
        JdxReplWsSrv_Merge_Test test = new JdxReplWsSrv_Merge_Test();
        test.setUp();
        test.test_LicDoc_MergeCommand();
    }

    @Test
    // Создиние дубликатов на работающей системе и их удаление
    // на сервере, через jc-команду
    public void test_CommentTip_jc() throws Exception {
        JdxReplWsSrv_Merge_Test test = new JdxReplWsSrv_Merge_Test();
        test.setUp();
        test.test_CommentTip_jc();
    }


}
