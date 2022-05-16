package jdtx.repl.main.api;

import jandcode.utils.variant.*;
import jdtx.repl.main.api.jdx_db_object.*;
import org.junit.*;

/**
 * Набор тестов для проверки после смены версии БД (или значительного изменения приложения).
 * До начала рефакторинга:
 * 1.
 * Сохранить в архиве рабочий каталог СТАРОЙ версии приложения
 * (см. пример test/jdtx/repl/main/api/JdxReplWsSrv_Verdb_Test.06.zip),
 * чтобы после рефакторинга ПРОДОЛЖАТЬ репликацию, но с обновлением версии.
 * 2.
 * Написать новый тест с восстановлением рабочего каталога СТАРОЙ версии приложения, по образцу test_Restore_06_Run.
 */
public class JdxReplWsSrv_Verdb_Test extends JdxReplWsSrv_Test {

    /**
     * Работа с предварительным восстановлением рабочего каталога
     * СТАРОЙ версии приложения
     */
    @Test
    public void test_Restore_06_Run() throws Exception {
        // Восстанавливаем все
        doRestore_06();

        // Просто работа
        test_AllHttp();
        test_AllHttp();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    /**
     * Просто работа, на том что есть сейчас.
     */
    @Test
    public void test_Run() throws Exception {
        test_AllHttp();
        test_AllHttp();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    /**
     * Восстанавливаем все и показываем
     */
    @Test
    public void test_Restore() throws Exception {
        doRestore_06();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    /**
     * Инициализация в версии 6,
     * а потом просто работа (чтобы можно было забрать в архив рабочий каталог).
     * Не забыть откатить исходники до версии "Релиз 670" в репозитарии.
     */
    @Test
    public void test_Run_Ver6() throws Exception {
        //
        UtDbObjectManager.CURRENT_VER_DB = 6;
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        test_AllHttp();
        test_AllHttp();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    /**
     * Восстанавливаем состояние тестового каталога и баз из архива
     */
    void doRestore_06() throws Exception {
        disconnectAllForce();
        clearAllTestData();
        // Рабочие каталоги
        UtZip.doUnzipDir("test/jdtx/repl/main/api/JdxReplWsSrv_Verdb_Test.06.zip", "../");
        // Создаем ящики рабочих станций
        IVariantMap args = new VariantMap();
        args.put("create", true);
        assertEquals("Ящики не созданы", true, extSrv.repl_mail_check(args));
        //
        connectAll();
    }


}
