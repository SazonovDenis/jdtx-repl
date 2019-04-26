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
public class ReplDatabase_Test extends AppTestCase {

    // Экземпляры db и db2, db3
    protected Db db;
    protected Db db2;
    protected Db db3;

    public void setUp() throws Exception {
        //
        super.setUp();

        //
        UtLog.loadProperties("../_log.properties");
        logOn();

        //
        Model m = app.getApp().service(ModelService.class).getModel();
        //
        db = m.getDb();
        db.connect();


        //
        Model m2 = app.getApp().service(ModelService.class).getModel("db2");
        //
        db2 = m2.getDb();
        db2.connect();


        //
        Model m3 = app.getApp().service(ModelService.class).getModel("db3");
        //
        db3 = m3.getDb();
        db3.connect();


        // ---
        // Чтобы были
        UtFile.mkdirs("temp");
    }

    /**
     * Нужен Firebird 2.0 и выше
     * @throws Exception
     */
    @Test
    public void test_db_user_context() throws Exception {
        DataStore st2 ;

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
    public void test_db_select() throws Exception {
        // db
        DataStore st = db.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st);
        // db2
        DataStore st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st2);
        // db3
        DataStore st3 = db3.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st3);
    }

    @Test
    public void test_trn_isolation() throws Exception {
        // Проверим, что db2 и db3 подключились к одной и той же БД
        db2.execSql("update dbInfo set dbLabel = '---'");
        db3.execSql("update dbInfo set dbLabel = '---'");

        //
        DataStore st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        String dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        DataStore st3 = db3.loadSql("select id, orgName, dbLabel from dbInfo");
        String dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db3.dbLabel: " + dbLabel_3);
        //
        assertEquals("Базы не однаковые", dbLabel_2, dbLabel_3);


        //
        db2.execSql("update dbInfo set dbLabel = '654321'");
        db3.execSql("update dbInfo set dbLabel = '123456'");

        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        st3 = db3.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db3.dbLabel: " + dbLabel_3);
        //
        assertEquals("Базы не однаковые", dbLabel_2, dbLabel_3);


        // ---
        db3.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        System.out.println("db2.TransactionIsolation: " + db2.getConnection().getTransactionIsolation());
        System.out.println("db3.TransactionIsolation: " + db3.getConnection().getTransactionIsolation());

        // Проверим уровни изоляции
        db3.startTran();
        System.out.println("db3.startTran");

        //
        db2.startTran();
        System.out.println("db2.startTran");


        //
        db2.execSql("update dbInfo set dbLabel = '654321'");
        System.out.println("db2.update");

        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        st3 = db3.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db3.dbLabel: " + dbLabel_3);
        //
        assertEquals("db2: не видим свои изменения", "654321", dbLabel_2);
        assertEquals("db3: видим чужие незакомиченные изменения", "123456", dbLabel_3);

        //
        db2.commit();
        System.out.println("db2.commit");

        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        st3 = db3.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db3.dbLabel: " + dbLabel_3);
        //
        if (dbLabel_3.compareTo("654321") == 0) {
            System.out.println("db3.isolation: READ_COMMITTED");
        } else {
            System.out.println("db3.isolation: REPEATABLE_READ");
        }
        //assertEquals("db3: видим чужие закомиченные изменения", "123456", dbLabel_3);

        //
        db3.commit();
        System.out.println("db3.commit");

        //
        st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_2 = st2.getCurRec().getValueString("dbLabel");
        System.out.println("db2.dbLabel: " + dbLabel_2);
        //
        st3 = db3.loadSql("select id, orgName, dbLabel from dbInfo");
        dbLabel_3 = st3.getCurRec().getValueString("dbLabel");
        System.out.println("db3.dbLabel: " + dbLabel_3);
        //
        assertEquals("db3: не видим чужие изменения", "654321", dbLabel_3);
    }


}
