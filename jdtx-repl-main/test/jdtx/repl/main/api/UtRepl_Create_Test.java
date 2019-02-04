package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.test.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 * Создание/удаление репликационных структур
 */
public class UtRepl_Create_Test extends DbmTestCase {


    public void setUp() throws Exception {
        super.setUp();

        // Копируем эталонную в рабочую
        String dbNameDest = app.getApp().getRt().getChild("db/default").getValue("database").toString();
        String dbNameSour = app.getApp().getRt().getChild("db/default").getValue("database_etalon").toString();
        File fDest = new File(dbNameDest);
        File fSour = new File(dbNameSour);
        FileUtils.copyFile(fSour, fDest);
        System.out.println("База подготовлена [" + dbNameDest + "]");

        //
        dbm.getDb().connect();
    }

    @Test
    public void test_db() throws Exception {
        DataStore st = dbm.getDb().loadSql("select id, orgName from dbInfo");
        dbm.outTable(st);
    }

    @Test
    public void test_dropReplication() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());
        utr.dropReplication();
    }

    @Test
    public void test_createReplication() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());
        utr.dropReplication();

        //
        JdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(dbm.getDb());
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();

        //
        IJdxDbStruct struct_1 = reader.readDbStruct(false);
        struct_rw.write(struct_1, "temp/dbStruct_1.xml");

        //
        utr.createReplication();
        //
        utr.dropReplication();

        //
        IJdxDbStruct struct_2 = reader.readDbStruct(false);
        struct_rw.write(struct_2, "temp/dbStruct_2.xml");

        // Проверим совпадение
        assertEquals("Количество таблиц", struct_1.getTables().size(), struct_2.getTables().size());
        for (int t = 0; t < struct_1.getTables().size(); t++) {
            assertEquals("Количество полей в таблице " + struct_1.getTables().get(t).getName(), struct_1.getTables().get(t).getFields().size(), struct_2.getTables().get(t).getFields().size());
            for (int f = 0; f < struct_1.getTables().get(t).getFields().size(); f++) {
                assertEquals("Полея в таблице " + struct_1.getTables().get(t).getName(), struct_1.getTables().get(t).getFields().get(f).getName(), struct_2.getTables().get(t).getFields().get(f).getName());
            }
        }

    }


}
