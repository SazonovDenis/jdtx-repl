package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.que.*;
import org.junit.*;

import java.io.*;

public class Update_010_011_dir_Test extends DbmTestCase {

    @Test
    public void test() throws Exception {
        logOn();
        //
        Db db = dbm.getDb();
        System.out.println(db.getDbSource().getDatabase());
        //
        SqlScriptExecutorService svc = app.service(SqlScriptExecutorService.class);
        ISqlScriptExecutor script = svc.createByName("Update_010_011_dir");
        script.exec(db);
    }

    @Test
    public void test_Dir() throws Exception {
        File file = new File("temp/test12345/file.zip");
        System.out.println("file: " + file.getAbsolutePath());
        file.getParentFile().mkdirs();
    }

    @Test
    public void test_getFileName() throws Exception {
        long[] n = new long[]{1, 99, 100, 101, 999, 1000, 1001, 1500, 2000, 7999, 8000, 9999, 10000, 10001, 10521, 10582, 15136};
        for (long no : n) {
            String fileNameNew = JdxStorageFile.getFileName(no);
            System.out.println(UtString.padLeft(String.valueOf(no), 5, ' ') + ", " + fileNameNew + ", " + JdxStorageFile.getNo(fileNameNew));
        }
    }

    @Test
    public void test_convertDir() throws Exception {
        logOn();
        //
        SqlScriptExecutorService svc = app.service(SqlScriptExecutorService.class);
        ISqlScriptExecutor script = svc.createByName("Update_010_011_dir");
        ((Update_010_011_dir) script).convertDir("../_test-data/_test-data_srv/ws_001/que_in");
    }

}
