package jdtx.repl.main.api.relocator;

import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.rec_merge.*;
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
    public void test_check() throws Exception {
        IdRelocator relocator = new IdRelocator(db, struct);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF"));

        //
        long idSour = 1357;
        long idDest = 200001357;
        System.out.println("Records updated for tables, referenced to " + "Lic" + ":");
        UtRecMerge.printRecordsUpdated(relocator.relocateIdCheck("Lic", idSour, idDest));

        //
        idSour = 200001357;
        idDest = 1357;
        try {
            System.out.println("Records updated for tables, referenced to " + "Lic" + ":");
            UtRecMerge.printRecordsUpdated(relocator.relocateIdCheck("Lic", idSour, idDest));
        } catch (Exception e) {
            if (e.getMessage().compareToIgnoreCase("No result in sqlrec") != 0) {
                throw e;
            }
        }
    }

    @Test
    public void test_relocate() throws Exception {
        IdRelocator relocator = new IdRelocator(db, struct);

        //
        System.out.println("===");
        System.out.println("Было");
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF"));

        //
        long idSour = 1357;
        long idDest = 200001357;
        relocator.relocateId("Lic", idSour, idDest);

        //
        System.out.println("===");
        System.out.println("Стало");
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF"));

        //
        idSour = 200001357;
        idDest = 1357;
        relocator.relocateId("Lic", idSour, idDest);

        //
        System.out.println("===");
        System.out.println("Вернулось");
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
        try {
            relocator.relocateId("Lic", idSour, idDest);
        } catch (Exception e) {
            if (e.getMessage().compareToIgnoreCase("No result in sqlrec") != 0) {
                throw e;
            }
        }

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));

        //
        idSour = 1357;
        idDest = 1357;
        try {
            relocator.relocateId("Lic", idSour, idDest);
            throw new XError("Нельзя перемещать в самого себя");
        } catch (Exception e) {
            if (!e.getMessage().contains("Error relocateId: idSour == idDest")) {
                throw e;
            }
        }

        //
        idSour = 0;
        idDest = 1357;
        try {
            relocator.relocateId("Lic", idSour, idDest);
            throw new XError("Нельзя перемещать id = 0");
        } catch (Exception e) {
            if (!e.getMessage().contains("Error relocateId: idSour == 0")) {
                throw e;
            }
        }


        //
        idSour = 1357;
        idDest = 0;
        try {
            relocator.relocateId("Lic", idSour, idDest);
            throw new XError("Нельзя перемещать id = 0");
        } catch (Exception e) {
            if (!e.getMessage().contains("Error relocateId: idDest == 0")) {
                throw e;
            }
        }

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));

        //
        idSour = 1357;
        idDest = 1358;
        try {
            relocator.relocateId("Lic", idSour, idDest);
            throw new XError("Нельзя перемещать в занятую");
        } catch (Exception e) {
            if (!e.getMessage().contains("violation of PRIMARY or UNIQUE KEY constraint")) {
                throw e;
            }
        }

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id"));
    }


}
