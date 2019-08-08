package jdtx.repl.main.api;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jandcode.utils.rt.*;
import jdtx.repl.main.ext.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 */
public class DbPrepareEtalon_Test extends AppTestCase {

    // Экземпляры db и db2, db3
    protected Db db;
    protected Db db2;
    protected Db db3;

    // Утилиты Jdx_Ext
    public Jdx_Ext extSrv;
    public Jdx_Ext extWs2;
    public Jdx_Ext extWs3;


    @Override
    public void setUp() throws Exception {
        //
        super.setUp();

        // Утилиты Jdx_Ext
        TestExtJc jc = createExt(TestExtJc.class);
        ProjectScript p1 = jc.loadProject("../ext/srv/project.jc");
        ProjectScript p2 = jc.loadProject("../ext/ws2/project.jc");
        ProjectScript p3 = jc.loadProject("../ext/ws3/project.jc");
        //
        extSrv = (Jdx_Ext) p1.createExt("jdtx.repl.main.ext.Jdx_Ext");
        extWs2 = (Jdx_Ext) p2.createExt("jdtx.repl.main.ext.Jdx_Ext");
        extWs3 = (Jdx_Ext) p3.createExt("jdtx.repl.main.ext.Jdx_Ext");

        // Экземпляры db и db2, db3
        db = extSrv.getApp().service(ModelService.class).getModel().getDb();
        db2 = extWs2.getApp().service(ModelService.class).getModel().getDb();
        db3 = extWs3.getApp().service(ModelService.class).getModel().getDb();
    }

    @Test
    public void test_PrepareEtalon() throws Exception {
        prepareEtalon();
    }


    /**
     * Копируем эталонную в рабочую
     */
    public void prepareEtalon() throws IOException {
        Rt rt = extSrv.getApp().getRt().getChild("db/default");
        String dbNameDest = rt.getValue("database").toString();
        String dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println("База подготовлена [" + dbNameDest + "]");

        //
        rt = extWs2.getApp().getRt().getChild("db/default");
        dbNameDest = rt.getValue("database").toString();
        dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println("База подготовлена [" + dbNameDest + "]");

        //
        rt = extWs3.getApp().getRt().getChild("db/default");
        dbNameDest = rt.getValue("database").toString();
        dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println("База подготовлена [" + dbNameDest + "]");
    }


}
