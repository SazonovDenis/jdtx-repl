package jdtx.repl.main.api;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import org.junit.*;

/**
 */
public class ReplDatabase_Test extends AppTestCase {

    // Экземпляры db и db2, db3
    Db db;
    Db db2;
    Db db3;

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


}
