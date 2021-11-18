package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_binder.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ext.*;
import org.junit.*;

import java.io.*;

public class JdxRecRelocator_Test extends DbmTestCase {

    Db db;
    IJdxDbStruct struct;

    @Override
    public void setUp() throws Exception {
        super.setUp();

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
    public void test_relocate() throws Exception {
        IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain();
        JdxRecRelocator relocator = new JdxRecRelocator(db, struct, dataSerializer);

        //
        String sql = "select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF";
        String sqlCheck = "select * from Lic order by NameF";

        //
        System.out.println("Было");
        DataStore st = db.loadSql(sql);
        UtData.outTable(st);

        //
        long id1 = st.get(1).getValueLong("id");
        String values1 = db.loadSql(sqlCheck).get(1).getValues().toString();

        //
        long idSour = id1;
        long idDest = 200001357;
        File outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        relocator.relocateId("Lic", idSour, idDest, outFile);

        //
        UtRecMergePrint.printMergeResults(outFile);

        //
        System.out.println("Перенос: " + idSour + " -> " + idDest);
        //
        assertEquals("Запись не исчезла: ", 0, db.loadSql("select max(id) id from Lic where id = " + idSour).get(0).getValueLong("id"));
        assertEquals("Запись не появилась: ", idDest, db.loadSql("select max(id) id from Lic where id = " + idDest).get(0).getValueLong("id"));

        //
        System.out.println();
        System.out.println("Стало");
        st = db.loadSql(sql);
        UtData.outTable(st);

        //
        idSour = 200001357;
        idDest = id1;
        outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        relocator.relocateId("Lic", idSour, idDest, outFile);

        //
        System.out.println("Перенос: " + idSour + " -> " + idDest);
        //
        assertEquals("Запись не исчезла: ", 0, db.loadSql("select max(id) id from Lic where id = " + idSour).get(0).getValueLong("id"));
        assertEquals("Запись не появилась: ", idDest, db.loadSql("select max(id) id from Lic where id = " + idDest).get(0).getValueLong("id"));

        //
        System.out.println();
        System.out.println("Вернулось");
        st = db.loadSql(sql);
        UtData.outTable(st);

        //
        String values2 = db.loadSql(sqlCheck).get(1).getValues().toString();
        assertEquals("Записи не одинаковые: ", values1, values2);
    }


    @Test
    public void test_fail() throws Exception {
        IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain();
        JdxRecRelocator relocator = new JdxRecRelocator(db, struct, dataSerializer);

        //
        String sql = "select id, NameF, NameI, NameO, BornDt, DocDt from Lic order by NameF";

        //
        DataStore st = db.loadSql(sql);
        UtData.outTable(st);

        //
        long id1 = st.get(1).getValueLong("id");
        long id2 = st.get(2).getValueLong("id");

        //
        long idSour = 9998;
        long idDest = 9999;
        File outFile = new File("temp/relocate_Lic_" + idSour + "_" + idDest + ".zip");
        try {
            relocator.relocateId("Lic", idSour, idDest, outFile);
        } catch (Exception e) {
            if (e.getMessage().compareToIgnoreCase("Error relocateId: idSour not found") != 0) {
                throw e;
            }
        }

        //
        st = db.loadSql(sql);
        UtData.outTable(st);

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
        st = db.loadSql(sql);
        UtData.outTable(st);

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
        st = db.loadSql(sql);
        UtData.outTable(st);
    }


    @Test
    public void test_relocate_all() throws Exception {
        String tableNames = "Lic,LicDocTip,LicDocVid,Ulz,UlzTip,Region,RegionTip";
        String[] tableNamesArr = tableNames.split(",");
        //
        int maxPkValue = 100000000;
        IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain();
        JdxRecRelocator relocator = new JdxRecRelocator(db, struct, dataSerializer);


        // Создадим себе проблему
        for (String tableName : tableNamesArr) {
            long idSour = db.loadSql("select min(id) id from " + tableName + " where id <> 0").getCurRec().getValueLong("id");
            long idDest = maxPkValue + idSour * 2;
            relocator.relocateId(tableName, idSour, idDest, new File("temp/1.zip"));
        }
        //
        for (String tableName : tableNamesArr) {
            String sql = "select * from " + tableName + " order by id desc";
            DataStore st = db.loadSql(sql);
            UtData.outTable(st, 5);
        }


        // Героически ее решим
        for (String tableName : tableNamesArr) {
            relocator.relocateIdAll(tableName, maxPkValue, "temp/");
        }


        //
        System.out.println();
        System.out.println("Стало:");
        for (String tableName : tableNamesArr) {
            String sql = "select * from " + tableName + " order by id desc";
            DataStore st = db.loadSql(sql);
            UtData.outTable(st, 5);
        }
    }

}
