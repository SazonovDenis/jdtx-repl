package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 */
public class ReplDatabaseStruct_Test extends DbPrepareEtalon_Test {

    // Структуры
    public IJdxDbStruct struct;
    public IJdxDbStruct struct2;
    public IJdxDbStruct struct3;
    public IJdxDbStruct struct5;


    public void setUp() throws Exception {
        //
        super.setUp();

        //
        UtLog.loadProperties("../_log.properties");
        logOn();


        //
        db.connect();
        db2.connect();
        db3.connect();
        db5.connect();


        // ---
        // Чтение структур
        reloadStruct_forTest();


        // ---
        // Чтобы были
        UtFile.mkdirs("temp");
    }

    // Чтение структур
    public void reloadStruct_forTest() throws Exception {
        IJdxDbStructReader reader = new JdxDbStructReader();
        //
        reader.setDb(db);
        struct = reader.readDbStruct();
        //
        reader.setDb(db2);
        struct2 = reader.readDbStruct();
        //
        reader.setDb(db3);
        struct3 = reader.readDbStruct();
        //
        reader.setDb(db5);
        struct5 = reader.readDbStruct();
    }

    @Test
    public void test_db_select() throws Exception {
        // db1
        DataStore st = db.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st);
        // db2
        DataStore st2 = db2.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st2);
        // db3
        DataStore st3 = db3.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st3);
        // db5
        DataStore st5 = db5.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st5);
    }

}
