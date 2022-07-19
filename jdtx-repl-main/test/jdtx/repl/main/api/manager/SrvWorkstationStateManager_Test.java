package jdtx.repl.main.api.manager;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.jdx_db_object.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class SrvWorkstationStateManager_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_verdb() throws Exception {
        UtDbObjectManager objectManager = new UtDbObjectManager(db);
        objectManager.CURRENT_VER_DB = 16;
        objectManager.checkReplVerDb();
    }

    @Test
    public void test_verdb_stepFill() throws Exception {
        Update_015_016_srv_workstation_state upd = new Update_015_016_srv_workstation_state();
        upd.exec(db);
    }

    @Test
    public void test_setValues() throws Exception {
        SrvWorkstationStateManager manager = new SrvWorkstationStateManager(db);

        String[] param_names = UtDbObjectManager.param_names;

        // Читаем сейчас
        Map<String, Long> values = new HashMap<>();
        for (String name : param_names) {
            long value = manager.getValue(2, name);
            values.put(name, value);
        }

        // Меняем
        for (String name : param_names) {
            long value = manager.getValue(2, name);
            manager.setValue(2, name, value + 2);
        }

        // Проверяем
        for (String name : param_names) {
            long value = manager.getValue(2, name);
            assertEquals(values.get(name) + 2, value);
        }

        // Проверяем что падает, где надо
        try {
            manager.getValue(2, "qwerty");
            throw new Exception("should fail");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if (e.getMessage().contains("should fail")) {
                throw e;
            }
        }

        try {
            manager.getValue(999, param_names[0]);
            throw new Exception("should fail");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if (e.getMessage().contains("should fail")) {
                throw e;
            }
        }

    }

}
