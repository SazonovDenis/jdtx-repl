package jdtx.repl.main.api;

import jandcode.utils.error.*;
import org.junit.*;

/**
 *
 */
public class JdxReplSrv_Test extends ReplDatabaseStruct_Test {


    /**
     * Проверяем: сервер не должен подключаться к рабочей станции
     */
    @Test
    public void test_init() throws Exception {
        JdxReplSrv srv1 = new JdxReplSrv(db);
        srv1.init();

        try {
            JdxReplSrv srv2 = new JdxReplSrv(db2);
            srv2.init();
        } catch (Exception e) {
            if (!e.getMessage().contains("Invalid server ws_id:")) {
                throw new XError("Сервер не должен подключаться к рабочей станции");
            }
            System.out.println("Сервер не подключился к рабочей станции");
        }
    }


}
