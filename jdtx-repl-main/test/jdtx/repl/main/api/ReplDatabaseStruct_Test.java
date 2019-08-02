package jdtx.repl.main.api;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ext.*;
import org.junit.*;

/**
 */
public class ReplDatabaseStruct_Test extends AppTestCase {

    // Структуры
    public IJdxDbStruct struct;
    public IJdxDbStruct struct2;
    public IJdxDbStruct struct3;

    // Экземпляры db и db2, db3
    protected Db db;
    protected Db db2;
    protected Db db3;

    // Утилиты Jdx_Ext
    public Jdx_Ext extSrv;
    public Jdx_Ext extWs2;
    public Jdx_Ext extWs3;

    public void setUp() throws Exception {
        //
        super.setUp();

        //
        UtLog.loadProperties("../_log.properties");
        logOn();

        // ---
        // Экземпляры db и db2, db3
        Model m = app.getApp().service(ModelService.class).getModel();
        Model m2 = app.getApp().service(ModelService.class).getModel("db2");
        Model m3 = app.getApp().service(ModelService.class).getModel("db3");
        db = m.getDb();
        db2 = m2.getDb();
        db3 = m3.getDb();
        db.connect();
        db2.connect();
        db3.connect();


        // ---
        // Чтение структур
        reloadStruct_forTest();


        // ---
        // Утилиты Jdx_Ext
        TestExtJc jc = createExt(TestExtJc.class);
        ProjectScript p1 = jc.loadProject("../ext/srv/project.jc");
        ProjectScript p2 = jc.loadProject("../ext/ws2/project.jc");
        ProjectScript p3 = jc.loadProject("../ext/ws3/project.jc");
        extSrv = (Jdx_Ext) p1.createExt("jdtx.repl.main.ext.Jdx_Ext");
        extWs2 = (Jdx_Ext) p2.createExt("jdtx.repl.main.ext.Jdx_Ext");
        extWs3 = (Jdx_Ext) p3.createExt("jdtx.repl.main.ext.Jdx_Ext");


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
    }

}
