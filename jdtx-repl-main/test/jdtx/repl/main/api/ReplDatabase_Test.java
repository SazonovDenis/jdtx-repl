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

    // Экземпляры db и db1
    Db db;
    Db db1;
    Db db2;

    public void setUp() throws Exception {
        //
        super.setUp();

        //
        Model m = app.getApp().service(ModelService.class).getModel();
        //
        db = m.getDb();
        db.connect();


        //
        Model m1 = app.getApp().service(ModelService.class).getModel("db1");
        //
        db1 = m1.getDb();
        db1.connect();


        //
        Model m2 = app.getApp().service(ModelService.class).getModel("db2");
        //
        db2 = m2.getDb();
        db2.connect();


        // ---
        // Чтобы были
        UtFile.mkdirs("temp");
    }

    @Test
    public void test_db_select() throws Exception {
        // db
        DataStore st = db.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st);
        // db1
        DataStore st1 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st1);
        // db2
        DataStore st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st2);
    }


}
