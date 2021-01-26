package jdtx.repl.main.api.relocator;

import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ext.*;
import org.junit.*;

public class IdRelocator_Test extends DbmTestCase {

    Db db;
    IJdxDbStruct struct;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        //
        // Утилиты Jdx_Ext
        TestExtJc jc = createExt(TestExtJc.class);
        ProjectScript p2 = jc.loadProject("../../ext/ws2/project.jc");
        //
        Jdx_Ext extWs2 = (Jdx_Ext) p2.createExt("jdtx.repl.main.ext.Jdx_Ext");

        // Экземпляры db2
        db = extWs2.getApp().service(ModelService.class).getModel().getDb();
        db.connect();
        System.out.println("db: " + db.getDbSource().getDatabase());
        //
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
    }

    @Test
    public void test_normal() throws Exception {
        IdRelocator relocator = new IdRelocator(db, struct);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF"));

        //
        long idSour = 1357;
        long idDest = 200001357;
        relocator.relocateId("Lic", idSour, idDest);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF"));

        //
        idSour = 200001357;
        idDest = 1357;
        relocator.relocateId("Lic", idSour, idDest);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF"));
    }



    @Test
    public void test_fail() throws Exception {
        IdRelocator relocator = new IdRelocator(db, struct);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));

        //
        long idSour = 9998;
        long idDest = 9999;
        relocator.relocateId("Lic", idSour, idDest);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));

        //
        idSour = 1357;
        idDest = 1357;
        relocator.relocateId("Lic", idSour, idDest);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));

        //
        idSour = 1357;
        idDest = 1358;
        relocator.relocateId("Lic", idSour, idDest);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));
    }


}
