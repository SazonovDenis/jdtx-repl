package jdtx.repl.main.api.ref_manager;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import org.junit.*;

public class RefManagerService_Test extends AppTestCase {

    protected Db db;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        //
        Model model = app.getApp().service(ModelService.class).getModel();
        db = model.getDb();
        db.connect();
    }

    @Test
    public void testSvc() throws Exception {
        IRefManager refManager = app.service(RefManagerService.class);
        System.out.println("RefManagerService: " + refManager.getClass().getName());

        //
        System.out.println(refManager.get_ref("Ulz", 1L));
    }

}
