package jdtx.repl.main.api.jdx_db_object;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

public class UtDbObjectManager_Test extends Database_Test {

    @Test
    public void test_checkReplVerDb() throws Exception {
        IDbObjectManager objectManager = DbToolsService.getDbObjectManager(db1);
        objectManager.checkVerDb();
    }

    @Test
    public void test_db_lock() throws Exception {
        IDbObjectManager objectManager_1 = DbToolsService.getDbObjectManager(db1);
        IDbObjectManager objectManager_2 = DbToolsService.getDbObjectManager(db2);

        // Тренируемся
        objectManager_1.lockDb();
        // Пытаемся поставить блокировку 1 еще раз
        try {
            objectManager_1.lockDb();
            throw new Exception("Неуместная успешная блокировка 1");
        } catch (Exception e) {
            if (!e.getMessage().contains("already locked")) {
                throw e;
            }
        }
        objectManager_1.unlockDb();

        // Ставим блокировку 1
        objectManager_1.lockDb();

        // Пытаемся поставить блокировку 2
        try {
            objectManager_2.lockDb();
            throw new Exception("Неуместная успешная блокировка 2");
        } catch (Exception e) {
            if (!e.getMessage().contains("already locked")) {
                throw e;
            }
        }

        // Снимаем блокировку 1
        objectManager_1.unlockDb();

        // Ставим блокировку 2
        objectManager_2.lockDb();

        // Пытаемся поставить блокировку 1
        try {
            objectManager_1.lockDb();
            throw new Exception("Неуместная успешная блокировка 1");
        } catch (Exception e) {
            if (!e.getMessage().contains("already locked")) {
                throw e;
            }
        }

        // Снимаем блокировку 2, путем disconnect
        db2.disconnectForce();

        // Ставим блокировку 1
        objectManager_1.lockDb();
    }

    @Test
    public void test_db_lock_1() throws Exception {
        IDbObjectManager objectManager_1 = DbToolsService.getDbObjectManager(db1);

        // Ставим блокировку 1
        objectManager_1.lockDb();
        System.out.println("objectManager_1.lockDb");

        // Снимаем блокировку 1
        objectManager_1.unlockDb();
        System.out.println("objectManager_1.unlockDb");
    }

    @Test
    public void test_db_lock_disconnect() throws Exception {
        IDbObjectManager objectManager_1 = DbToolsService.getDbObjectManager(db1);

        // Ставим блокировку 1
        objectManager_1.lockDb();
        System.out.println("objectManager_1.lockDb");

        // Снимаем блокировку 1
        db1.disconnectForce();
        System.out.println("objectManager_1.unlockDb");
    }

    @Test
    public void test_db_lock_2() throws Exception {
        IDbObjectManager objectManager_2 = DbToolsService.getDbObjectManager(db2);

        // Ставим блокировку 1
        objectManager_2.lockDb();
        System.out.println("objectManager_2.lockDb");

        // Снимаем блокировку 1
        objectManager_2.unlockDb();
        System.out.println("objectManager_2.unlockDb");
    }


}
