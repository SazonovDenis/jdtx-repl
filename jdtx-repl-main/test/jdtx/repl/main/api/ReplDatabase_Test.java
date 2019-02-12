package jdtx.repl.main.api;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 */
public class ReplDatabase_Test extends AppTestCase {

    UtTest utTest;

    Db db;
    Db db1;

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


        // ---
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();
        utTest = new UtTest(db, struct);


        // ---
        // Чтобы были
        UtFile.mkdirs("temp");

    }

    @Test
    public void doPrepareEtalon() throws Exception {
        db.disconnectForce();
        db.getDbSource().getConnectionService().disconnectAll();
        db1.disconnectForce();
        db1.getDbSource().getConnectionService().disconnectAll();

        // ---
        // --- Готовим базу

        // Копируем эталонную в рабочую
        String dbNameDest = app.getApp().getRt().getChild("db/default").getValue("database").toString();
        String dbNameSour = app.getApp().getRt().getChild("db/default").getValue("database_etalon").toString();
        File fDest = new File(dbNameDest);
        File fSour = new File(dbNameSour);
        FileUtils.copyFile(fSour, fDest);

        //
        System.out.println("База подготовлена [" + dbNameDest + "]");


        // ---
        String dbNameDest1 = app.getApp().getRt().getChild("db/db1").getValue("database").toString();
        String dbNameSour1 = app.getApp().getRt().getChild("db/db1").getValue("database_etalon").toString();
        fDest = new File(dbNameDest1);
        fSour = new File(dbNameSour1);
        FileUtils.copyFile(fSour, fDest);

        //
        System.out.println("База подготовлена [" + dbNameDest1 + "]");


        // ---
        Model m1 = app.getApp().service(ModelService.class).getModel("db1");
        //
        db1 = m1.getDb();
        db1.connect();

        //
        Model m = app.getApp().service(ModelService.class).getModel();
        db = m.getDb();
        db.connect();
    }


}
