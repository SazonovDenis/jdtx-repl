package jdtx.repl.main.api;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import org.junit.*;

import java.sql.*;

/**
 */
public class Database_Test extends AppTestCase {

    // Два коннекта к одной БД
    protected Db db1;
    protected Db db2;


    public void setUp() throws Exception {
        //
        super.setUp();

        //
        UtLog.loadProperties("../_log.properties");
        logOn();

        // db1 - первый коннект к БД
        Model m1 = app.getApp().service(ModelService.class).getModel("default");
        db1 = m1.getDb();
        db1.connect();


        // db2 - второй коннект к БД
        Model m2 = app.getApp().service(ModelService.class).getModel("db2");
        db2 = m2.getDb();
        db2.getDbSource().setDbType(db1.getDbSource().getDbType());
        db2.getDbSource().setDbDriver(db1.getDbSource().getDbDriver().getName());
        db2.getDbSource().setJdbcDriverClass(db1.getDbSource().getJdbcDriverClass());
        db2.getDbSource().setUrl(db1.getDbSource().getUrl());
        db2.getDbSource().setDatabase(db1.getDbSource().getDatabase());
        db2.getDbSource().setHost(db1.getDbSource().getHost());
        db2.getDbSource().setUsername(db1.getDbSource().getUsername());
        db2.getDbSource().setPassword(db1.getDbSource().getPassword());
        db2.connect();
    }

    /**
     * Нужен Firebird 2.0 и выше
     *
     * @throws Exception
     */
    @Test
    public void test_db_user_context() throws Exception {
        DataStore st2;

        //
        st2 = db2.loadSql("select rdb$get_context('USER_SESSION', 'MY') as MY from rdb$database");
        UtData.outTable(st2);

        // db
        db2.execSql("select rdb$set_context('USER_SESSION', 'MY', 'это моя крутая переменая') from rdb$database");

        //
        st2 = db2.loadSql("select rdb$get_context('USER_SESSION', 'MY') as MY from rdb$database");
        UtData.outTable(st2);

        // db
        db2.execSql("select rdb$set_context('USER_SESSION', 'MY', NULL) from rdb$database");

        //
        st2 = db2.loadSql("select rdb$get_context('USER_SESSION', 'MY') as MY from rdb$database");
        UtData.outTable(st2);
    }

    @Test
    public void test_trn_isolation() throws Exception {

        // ---
        // Проверим, что db2 и db1 - это разные экземпляры
        db2.disconnect();
        db1.loadSql("select * from dbInfo");
        db2.connect();

        db1.disconnect();
        db2.loadSql("select * from dbInfo");
        db1.connect();

        db2.disconnect();
        try {
            db2.loadSql("select * from dbInfo");
            throw new Exception("should fail");
        } catch (Exception e) {
            if (!UtJdx.collectExceptionText(e).contains("Соединение не установлено")) {
                throw e;
            }
        }
        db2.connect();


        // ---
        // Проверим, что db2 и db1 подключились к одной и той же БД

        // Пишем одинаковое значение
        db1.execSql("update dbInfo set dbLabel = '---'");
        db2.execSql("update dbInfo set dbLabel = '---'");

        //
        DataStore st1 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        String dbLabel_1 = st1.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_1);
        //
        DataStore st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        String dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        assertEquals("Коннекты к разным базам", dbLabel_1, dbLabel_2);


        // Пишем разные значения
        db1.execSql("update dbInfo set dbLabel = '123456'");
        db2.execSql("update dbInfo set dbLabel = '234567'");

        //
        st1 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_1 = st1.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_1);
        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        assertEquals("Коннекты к разным базам", dbLabel_1, dbLabel_2);


        // ---
        // Установим уровни изоляции
        db1.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        //db2.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        System.out.println("db1.TransactionIsolation: " + db1.getConnection().getTransactionIsolation());
        System.out.println("db2.TransactionIsolation: " + db2.getConnection().getTransactionIsolation());


        // ---
        // Проверим уровни изоляции
        db1.execSql("update dbInfo set dbLabel = '---'");
        db2.execSql("update dbInfo set dbLabel = '---'");
        //
        db1.startTran();
        System.out.println("db1.startTran");
        //
        db2.startTran();
        System.out.println("db2.startTran");

        // Состояние до изменений
        st1 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_1 = st1.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_1);
        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);

        // db2 пишет, db1 читает
        db2.execSql("update dbInfo set dbLabel = '234567'");
        System.out.println("db2.update");

        // Состояние до коммита изменений от db2
        st1 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_1 = st1.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_1);
        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        assertEquals("db1: видим чужие незакомиченные изменения", "---", dbLabel_1);
        assertEquals("db2: не видим свои изменения", "234567", dbLabel_2);

        //
        db2.commit();
        System.out.println("db2.commit");

        // Состояние после коммита изменений от db2
        st1 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_1 = st1.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_1);
        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        if (dbLabel_1.compareTo("234567") == 0) {
            System.out.println("db1.isolation: READ_COMMITTED");
        } else {
            System.out.println("db1.isolation: REPEATABLE_READ");
        }
        assertEquals("db1: видим чужие закомиченные изменения", "---", dbLabel_1);

        //
        db1.commit();
        System.out.println("db1.commit");

        // Состояние после коммита db1
        st1 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_1 = st1.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_1);
        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        assertEquals("db1: не видим чужие закомиченные изменения", "234567", dbLabel_1);
    }


    @Test
    public void test_trn_isolation_ok() throws Exception {
        // Проверим, что db1 и db2 подключились к одной и той же БД
        db1.execSql("update dbInfo set dbLabel = '---'");
        db2.execSql("update dbInfo set dbLabel = '---'");

        //
        DataStore st2 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        String dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_2);
        //
        DataStore st3 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        String dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_3);
        //
        assertEquals("Базы не однаковые", dbLabel_2, dbLabel_3);


        //
        db1.execSql("update dbInfo set dbLabel = '654321'");
        db2.execSql("update dbInfo set dbLabel = '123456'");

        //
        st2 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_2);
        //
        st3 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_3);
        //
        assertEquals("Базы не однаковые", dbLabel_2, dbLabel_3);


        // ---
        db1.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        db2.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        System.out.println("db1.TransactionIsolation: " + db1.getConnection().getTransactionIsolation());
        System.out.println("db2.TransactionIsolation: " + db2.getConnection().getTransactionIsolation());

        // Проверим уровни изоляции
        db2.startTran();
        System.out.println("db2.startTran");

        //
        db1.startTran();
        System.out.println("db1.startTran");


        // db1 пишет, db2 читает
        db1.execSql("update dbInfo set dbLabel = '654321'");
        System.out.println("db1.update");

        //
        st2 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_2);
        //
        st3 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_3);
        //
        assertEquals("db1: не видим свои изменения", "654321", dbLabel_2);
        assertEquals("db2: видим чужие незакомиченные изменения", "123456", dbLabel_3);

        //
        db1.commit();
        System.out.println("db1.commit");

        //
        st2 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_2);
        //
        st3 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_3);
        //
        if (dbLabel_3.compareTo("654321") == 0) {
            System.out.println("db2.isolation: READ_COMMITTED");
        } else {
            System.out.println("db2.isolation: REPEATABLE_READ");
        }
        //assertEquals("db2: видим чужие закомиченные изменения", "123456", dbLabel_3);

        //
        db2.commit();
        System.out.println("db2.commit");

        //
        st2 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db1.dbLabel: " + dbLabel_2);
        //
        st3 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_3);
        //
        assertEquals("db2: не видим чужие изменения", "654321", dbLabel_3);
    }

}
