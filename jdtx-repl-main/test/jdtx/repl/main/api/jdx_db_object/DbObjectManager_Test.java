package jdtx.repl.main.api.jdx_db_object;

import jdtx.repl.main.api.*;
import org.junit.*;

public class DbObjectManager_Test extends DBTransactionsIsolation_Test {

    @Test
    public void test_checkReplVerDb() throws Exception {
        IDbObjectManager dbObjectManager = db1.service(DbObjectManager.class);
        dbObjectManager.checkVerDb();
    }

    @Test
    public void test_db_lock() throws Exception {
        IDbObjectManager dbObjectManager_1 = db1.service(DbObjectManager.class);
        IDbObjectManager dbObjectManager_2 = db2.service(DbObjectManager.class);

        // Тренируемся
        dbObjectManager_1.lockDb();
        // Пытаемся поставить блокировку 1 еще раз
        try {
            dbObjectManager_1.lockDb();
            throw new Exception("Неуместная успешная блокировка 1");
        } catch (Exception e) {
            if (!e.getMessage().contains("already locked")) {
                throw e;
            }
        }
        dbObjectManager_1.unlockDb();

        // Ставим блокировку 1
        dbObjectManager_1.lockDb();

        // Пытаемся поставить блокировку 2
        try {
            dbObjectManager_2.lockDb();
            throw new Exception("Неуместная успешная блокировка 2");
        } catch (Exception e) {
            if (!e.getMessage().contains("already locked")) {
                throw e;
            }
        }

        // Снимаем блокировку 1
        dbObjectManager_1.unlockDb();

        // Ставим блокировку 2
        dbObjectManager_2.lockDb();

        // Пытаемся поставить блокировку 1
        try {
            dbObjectManager_1.lockDb();
            throw new Exception("Неуместная успешная блокировка 1");
        } catch (Exception e) {
            if (!e.getMessage().contains("already locked")) {
                throw e;
            }
        }

        // Снимаем блокировку 2, путем disconnect
        db2.disconnectForce();

        // Ставим блокировку 1
        dbObjectManager_1.lockDb();
    }

    @Test
    public void test_db_lock_1() throws Exception {
        IDbObjectManager dbObjectManager_1 = db1.service(DbObjectManager.class);

        // Ставим блокировку 1
        dbObjectManager_1.lockDb();
        System.out.println("dbObjectManager_1.lockDb");

        // Снимаем блокировку 1
        dbObjectManager_1.unlockDb();
        System.out.println("dbObjectManager_1.unlockDb");
    }

    @Test
    public void test_db_lock_disconnect() throws Exception {
        IDbObjectManager dbObjectManager_1 = db1.service(DbObjectManager.class);

        // Ставим блокировку 1
        dbObjectManager_1.lockDb();
        System.out.println("dbObjectManager_1.lockDb");

        // Снимаем блокировку 1
        db1.disconnectForce();
        System.out.println("dbObjectManager_1.unlockDb");
    }

    @Test
    public void test_db_lock_2() throws Exception {
        IDbObjectManager objectManager_2 = db2.service(DbObjectManager.class);

        // Ставим блокировку 1
        objectManager_2.lockDb();
        System.out.println("objectManager_2.lockDb");

        // Снимаем блокировку 1
        objectManager_2.unlockDb();
        System.out.println("objectManager_2.unlockDb");
    }


}
