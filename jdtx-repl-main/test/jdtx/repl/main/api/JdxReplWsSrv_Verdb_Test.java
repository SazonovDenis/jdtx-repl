package jdtx.repl.main.api;

import jandcode.utils.variant.*;
import jdtx.repl.main.api.jdx_db_object.*;
import org.junit.*;

import java.io.*;
import java.util.zip.*;

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
     * Инициализайия в версии 6,
     * а потом просто работа (чтобы можно было забрать в архив рабочий каталог)
     */
    @Test
    public void test_Run_Ver6() throws Exception {
        //
        UtDbObjectManager.CURRENT_VER_DB = 6;
        allSetUp();

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
        doDisconnectAllForce();
        clearAllTestData();
        // Рабочие каталоги
        doUnzip("test/jdtx/repl/main/api/JdxReplWsSrv_Verdb_Test.06.zip", "../");
        // Создаем ящики рабочих станций
        IVariantMap args = new VariantMap();
        args.put("create", true);
        assertEquals("Ящики не созданы", true, extSrv.repl_mail_check(args));
        //
        doConnectAll();
    }


    void doUnzip(String zipFilePath, String destDir) throws IOException {
        File dir = new File(destDir);
        // create output directory if it doesn't exist
        if (!dir.exists()) dir.mkdirs();
        FileInputStream fis;
        //buffer for read and write data to file
        byte[] buffer = new byte[1024];
        fis = new FileInputStream(zipFilePath);
        ZipInputStream zis = new ZipInputStream(fis);
        ZipEntry ze = zis.getNextEntry();
        while (ze != null) {
            if (!ze.isDirectory()) {
                String fileName = ze.getName();
                File newFile = new File(destDir + File.separator + fileName);
                System.out.println("Unzipping to " + newFile.getAbsolutePath());
                //create directories for sub directories in zip
                new File(newFile.getParent()).mkdirs();
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                //close this ZipEntry
                zis.closeEntry();
            }
            ze = zis.getNextEntry();
        }
        //close last ZipEntry
        zis.closeEntry();
        zis.close();
        fis.close();
    }

}
