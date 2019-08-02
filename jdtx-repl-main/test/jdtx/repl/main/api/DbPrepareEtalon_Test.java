package jdtx.repl.main.api;

import jandcode.app.*;
import jandcode.app.test.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 */
public class DbPrepareEtalon_Test extends AppTestCase {


    @Test
    public void test_PrepareEtalon() throws Exception {
        prepareEtalon(app.getApp());
    }


    /**
     * Копируем эталонную в рабочую
     */
    public static void prepareEtalon(App app) throws IOException {
        String dbNameDest = app.getRt().getChild("db/default").getValue("database").toString();
        String dbNameSour = app.getRt().getChild("db/default").getValue("database_etalon").toString();
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));

        //
        System.out.println("База подготовлена [" + dbNameDest + "]");


        // ---
        String dbNameDest2 = app.getRt().getChild("db/db2").getValue("database").toString();
        String dbNameSour2 = app.getRt().getChild("db/db2").getValue("database_etalon").toString();
        FileUtils.copyFile(new File(dbNameSour2), new File(dbNameDest2));

        //
        System.out.println("База подготовлена [" + dbNameDest2 + "]");


        // ---
        String dbNameDest3 = app.getRt().getChild("db/db3").getValue("database").toString();
        String dbNameSour3 = app.getRt().getChild("db/db3").getValue("database_etalon").toString();
        FileUtils.copyFile(new File(dbNameSour3), new File(dbNameDest3));

        //
        System.out.println("База подготовлена [" + dbNameDest3 + "]");
    }


}
