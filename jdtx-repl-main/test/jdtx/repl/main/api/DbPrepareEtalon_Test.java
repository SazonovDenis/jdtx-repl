package jdtx.repl.main.api;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 */
public class DbPrepareEtalon_Test extends AppTestCase {


    @Test
    /**
     * Копируем эталонную в рабочую
     */
    public void doPrepareEtalon() throws Exception {
        String dbNameDest = app.getApp().getRt().getChild("db/default").getValue("database").toString();
        String dbNameSour = app.getApp().getRt().getChild("db/default").getValue("database_etalon").toString();
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));

        //
        System.out.println("База подготовлена [" + dbNameDest + "]");


        // ---
        String dbNameDest1 = app.getApp().getRt().getChild("db/db1").getValue("database").toString();
        String dbNameSour1 = app.getApp().getRt().getChild("db/db1").getValue("database_etalon").toString();
        FileUtils.copyFile(new File(dbNameSour1), new File(dbNameDest1));


        // ---
        String dbNameDest2 = app.getApp().getRt().getChild("db/db2").getValue("database").toString();
        String dbNameSour2 = app.getApp().getRt().getChild("db/db2").getValue("database_etalon").toString();
        FileUtils.copyFile(new File(dbNameSour2), new File(dbNameDest2));

        //
        System.out.println("База подготовлена [" + dbNameDest2 + "]");
    }

    @Test
    public void test_connect() throws Exception {
        //
        Model m = app.getApp().service(ModelService.class).getModel();
        Db db = m.getDb();
        db.connect();

        //
        Model m1 = app.getApp().service(ModelService.class).getModel("db1");
        Db db1 = m1.getDb();
        db1.connect();

        //
        Model m2 = app.getApp().service(ModelService.class).getModel("db2");
        Db db2 = m2.getDb();
        db2.connect();
    }


}
