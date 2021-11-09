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

import java.io.*;

public class UtRecRelocator_Test extends DbmTestCase {

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
        UtRecRelocator relocator = new UtRecRelocator(db, struct);

        //
        UtData.outTable(db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF"));

        //
        long idSour = 1357;
        File outFile = new File("temp/relocateCheck_Lic_" + idSour + ".zip");
        relocator.relocateIdCheck("Lic", idSour, outFile);
        System.out.println("OutFile: " + outFile.getAbsolutePath());
        UtRecMergePrint.printMergeResults(outFile);
        System.out.println("");

        //
        idSour = 200001357;
        outFile = new File("temp/relocateCheck_Lic_" + idSour + ".zip");
        try {
            relocator.relocateIdCheck("Lic", idSour, outFile);
            System.out.println("OutFile: " + outFile.getAbsolutePath());
            UtRecMergePrint.printMergeResults(outFile);
            System.out.println("");
        } catch (Exception e) {
            if (e.getMessage().compareToIgnoreCase("No result in sqlrec") != 0) {
                throw e;
            }
        }
    }

    @Test
    public void test_relocate() throws Exception {
        UtRecRelocator relocator = new UtRecRelocator(db, struct);

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
        File outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        relocator.relocateId("Lic", idSour, idDest, outFile);

        //
        System.out.println("===");
        System.out.println("Стало");
        ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF");
        UtData.outTable(ds);

        //
        idSour = 200001357;
        idDest = id1;
        outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        relocator.relocateId("Lic", idSour, idDest, outFile);

        //
        System.out.println("===");
        System.out.println("Вернулось");
        ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF");
        UtData.outTable(ds);
    }


    @Test
    public void test_fail() throws Exception {
        UtRecRelocator relocator = new UtRecRelocator(db, struct);

        //
        DataStore ds = db.loadSql("select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by id");
        UtData.outTable(ds);

        //
        long id1 = ds.get(1).getValueLong("id");
        long id2 = ds.get(2).getValueLong("id");

        //
        long idSour = 9998;
        long idDest = 9999;
        File outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        try {
            relocator.relocateId("Lic", idSour, idDest, outFile);
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
        outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        try {
            relocator.relocateId("Lic", idSour, idDest, outFile);
            throw new XError("Нельзя перемещать в самого себя");
        } catch (Exception e) {
            if (!e.getMessage().contains("Error relocateId: idSour == idDest")) {
                throw e;
            }
        }

        //
        idSour = 0;
        idDest = id1;
        outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        try {
            relocator.relocateId("Lic", idSour, idDest, outFile);
            throw new XError("Нельзя перемещать id = 0");
        } catch (Exception e) {
            if (!e.getMessage().contains("Error relocateId: idSour == 0")) {
                throw e;
            }
        }


        //
        idSour = id1;
        idDest = 0;
        outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        try {
            relocator.relocateId("Lic", idSour, idDest, outFile);
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
        outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        try {
            relocator.relocateId("Lic", idSour, idDest, outFile);
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


    @Test
    public void test_relocate_all() throws Exception {
        String tableNames = "Lic,LicDocTip,LicDocVid,Ulz,UlzTip,Region,RegionTip";
        String[] tableNamesArr = tableNames.split(",");
        //
        int maxPkValue = 100000000;

        //
        UtRecRelocator relocator = new UtRecRelocator(db, struct);

        //
        for (String tableName : tableNamesArr) {
            relocator.relocateIdAll(tableName, maxPkValue, "temp/");
        }

    }

}
