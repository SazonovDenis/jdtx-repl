package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ext.*;
import org.junit.*;

public class UtRecMerge_Relocate_Test extends DbmTestCase {

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
        UtRecMerge relocator = new UtRecMerge(db, struct);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF"));

        //
        long idSour = 1357;
        long idDest = 200001357;
        MergeResultTable relocateCheckResult = relocator.recordsRelocateFindRefs("Lic", idSour);
        System.out.println("Record relocated:");
        UtData.outTable(relocateCheckResult.recordsDeleted);
        System.out.println("Records updated for tables, referenced to " + "Lic" + ":");
        UtRecMergePrint.printRecordsUpdated(relocateCheckResult.recordsUpdated);

        //
        idSour = 200001357;
        idDest = 1357;
        try {
            relocateCheckResult = relocator.recordsRelocateFindRefs("Lic", idSour);
            System.out.println("Record deleted:");
            UtData.outTable(relocateCheckResult.recordsDeleted);
            System.out.println("Records updated for tables, referenced to " + "Lic" + ":");
            UtRecMergePrint.printRecordsUpdated(relocateCheckResult.recordsUpdated);
        } catch (Exception e) {
            if (e.getMessage().compareToIgnoreCase("No result in sqlrec") != 0) {
                throw e;
            }
        }
    }

    @Test
    public void test_relocate() throws Exception {
        UtRecMerge relocator = new UtRecMerge(db, struct);

        //
        System.out.println("===");
        System.out.println("Было");
        DataStore ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF");
        UtData.outTable(ds);

        //
        long id1 = ds.get(1).getValueLong("id");

        //
        long idSour = id1;
        long idDest = 200001357;
        relocator.relocateId("Lic", idSour, idDest);

        //
        System.out.println("===");
        System.out.println("Стало");
        ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF");
        UtData.outTable(ds);

        //
        idSour = 200001357;
        idDest = id1;
        relocator.relocateId("Lic", idSour, idDest);

        //
        System.out.println("===");
        System.out.println("Вернулось");
        ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF");
        UtData.outTable(ds);
    }


    @Test
    public void test_fail() throws Exception {
        UtRecMerge relocator = new UtRecMerge(db, struct);

        //
        DataStore ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id");
        UtData.outTable(ds);

        //
        long id1 = ds.get(1).getValueLong("id");
        long id2 = ds.get(2).getValueLong("id");

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
        ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id");
        UtData.outTable(ds);

        //
        idSour = id1;
        idDest = id1;
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
        idDest = id1;
        try {
            relocator.relocateId("Lic", idSour, idDest);
            throw new XError("Нельзя перемещать id = 0");
        } catch (Exception e) {
            if (!e.getMessage().contains("Error relocateId: idSour == 0")) {
                throw e;
            }
        }


        //
        idSour = id1;
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
        ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id");
        UtData.outTable(ds);

        //
        idSour = id1;
        idDest = id2;
        try {
            relocator.relocateId("Lic", idSour, idDest);
            throw new XError("Нельзя перемещать в занятую");
        } catch (Exception e) {
            if (!e.getMessage().contains("violation of PRIMARY or UNIQUE KEY constraint")) {
                throw e;
            }
        }

        //
        ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id");
        UtData.outTable(ds);
    }


}
