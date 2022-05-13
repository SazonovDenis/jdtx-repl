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
 *
 */
public class DbPrepareEtalon_Test extends AppTestCase {


    // Экземпляры db, db2, db3 и db5
    protected Db db;
    protected Db db2;
    protected Db db3;
    protected Db db5;

    // Утилиты Jdx_Ext
    public Jdx_Ext extSrv;
    public Jdx_Ext extWs2;
    public Jdx_Ext extWs3;
    public Jdx_Ext extWs5;

    protected String rootDir = "../ext/";

    @Override
    public void setUp() throws Exception {
        //
        super.setUp();

        // Утилиты Jdx_Ext
        TestExtJc jc = createExt(TestExtJc.class);
        ProjectScript p1 = jc.loadProject(rootDir + "srv/project.jc");
        ProjectScript p2 = jc.loadProject(rootDir + "ws2/project.jc");
        ProjectScript p3 = jc.loadProject(rootDir + "ws3/project.jc");
        ProjectScript p5 = jc.loadProject(rootDir + "ws5/project.jc");
        //
        extSrv = (Jdx_Ext) p1.createExt("jdtx.repl.main.ext.Jdx_Ext");
        extWs2 = (Jdx_Ext) p2.createExt("jdtx.repl.main.ext.Jdx_Ext");
        extWs3 = (Jdx_Ext) p3.createExt("jdtx.repl.main.ext.Jdx_Ext");
        extWs5 = (Jdx_Ext) p5.createExt("jdtx.repl.main.ext.Jdx_Ext");

        // Экземпляры db, db2, db3 и db5
        db = extSrv.getApp().service(ModelService.class).getModel().getDb();
        db2 = extWs2.getApp().service(ModelService.class).getModel().getDb();
        db3 = extWs3.getApp().service(ModelService.class).getModel().getDb();
        db5 = extWs5.getApp().service(ModelService.class).getModel().getDb();
    }

    @Test
    public void prepareEtalon() throws Exception {
        doPrepareEtalon();
    }

    @Test
    public void test_ConnectAll() throws Exception {
        db.disconnect();
        db2.disconnect();
        db3.disconnect();
        db5.disconnect();

        //
        connectAll();

        //
        doDisconnectAll();
    }

    void connectAll() throws Exception {
        connectAll(true);
    }

    void connectAll(boolean doRaise) throws Exception {
        doConnect(db, doRaise);
        doConnect(db2, doRaise);
        doConnect(db3, doRaise);
        doConnect(db5, doRaise);
    }

    private void doConnect(Db db, boolean doRaise) throws Exception {
        try {
            db.connect();
            System.out.println("db.connect: " + db.getDbSource().getDatabase());
        } catch (Exception e) {
            if (doRaise) {
                throw e;
            }

            System.out.println("db.connect: " + e.getMessage());
        }
    }

    void doDisconnectAll() throws Exception {
        doDisconnectAll(false);
    }

    void doDisconnectAll(boolean doRaise) throws Exception {
        doDisconnect(db, doRaise);
        doDisconnect(db2, doRaise);
        doDisconnect(db3, doRaise);
        doDisconnect(db5, doRaise);
    }

    private void doDisconnect(Db db, boolean doRaise) throws Exception {
        try {
            db.disconnect();
            System.out.println("db.disconnect: " + db.getDbSource().getDatabase());
        } catch (Exception e) {
            if (doRaise) {
                throw e;
            }
            System.out.println("db.disconnect: " + e.getMessage());
        }
    }

    void disconnectAllForce() throws Exception {
        disconnectAllForce(false);
    }

    void disconnectAllForce(boolean doRaise) throws Exception {
        disconnectForce(db, doRaise);
        disconnectForce(db2, doRaise);
        disconnectForce(db3, doRaise);
        disconnectForce(db5, doRaise);
    }

    private void disconnectForce(Db db, boolean doRaise) throws Exception {
        try {
            db.disconnectForce();
            System.out.println("db.disconnectForce: " + db.getDbSource().getDatabase());
        } catch (Exception e) {
            if (doRaise) {
                throw e;
            }
            System.out.println("db.disconnectForce: " + e.getMessage());
        }
    }


    /**
     * Копируем эталонную в рабочую
     */
    public void doPrepareEtalon() throws IOException {
        Rt rt = extSrv.getApp().getRt().getChild("db/default");
        String dbNameDest = rt.getValue("database").toString();
        String dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println("Новая база скопирована [" + dbNameDest + "]");

        //
        rt = extWs2.getApp().getRt().getChild("db/default");
        dbNameDest = rt.getValue("database").toString();
        dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println("Новая база скопирована [" + dbNameDest + "]");

        //
        rt = extWs3.getApp().getRt().getChild("db/default");
        dbNameDest = rt.getValue("database").toString();
        dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println("Новая база скопирована [" + dbNameDest + "]");

        //
        rt = extWs5.getApp().getRt().getChild("db/default");
        dbNameDest = rt.getValue("database").toString();
        dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println("Новая база скопирована [" + dbNameDest + "]");
    }


}
