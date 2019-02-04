package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import jandcode.utils.*;
import org.apache.commons.io.*;

import java.io.*;

/**
 */
public class Repl_Test_Custom extends DbmTestCase {

    UtTest utt = new UtTest();

    public void setUp() throws Exception {
        //
        super.setUp();


        // --- Готовим базу

        // Копируем эталонную в рабочую
        String dbNameDest = app.getApp().getRt().getChild("db/default").getValue("database").toString();
        String dbNameSour = app.getApp().getRt().getChild("db/default").getValue("database_etalon").toString();
        File fDest = new File(dbNameDest);
        File fSour = new File(dbNameSour);
        FileUtils.copyFile(fSour, fDest);

        //
        dbm.getDb().connect();

        //
        System.out.println("База подготовлена [" + dbNameDest + "]");


        // ---
        UtFile.mkdirs("temp");

    }

}
