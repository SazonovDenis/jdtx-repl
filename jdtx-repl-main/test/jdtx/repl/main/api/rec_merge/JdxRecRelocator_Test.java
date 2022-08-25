package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ext.*;
import org.junit.*;

import java.io.*;
import java.util.*;

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
        String tempDirName = "temp/relocate/";
        UtFile.cleanDir(tempDirName);

        //
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
        File outFile = new File(tempDirName + "relocate_Lic_" + idSour + "_" + idDest + ".zip");
        try {
            relocator.relocateId("Lic", idSour, idDest, outFile);
        } catch (Exception e) {
            if (!e.getMessage().contains("Error relocateId: sour id not found:")) {
                throw e;
            }
        }

        //
        st = db.loadSql(sql);
        UtData.outTable(st);

        //
        idSour = id1;
        idDest = id1;
        outFile = new File(tempDirName + "relocate_Lic_" + idSour + "_" + idDest + ".zip");
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
        outFile = new File(tempDirName + "relocate_Lic_" + idSour + "_" + idDest + ".zip");
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
        outFile = new File(tempDirName + "relocate_Lic_" + idSour + "_" + idDest + ".zip");
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
        outFile = new File(tempDirName + "relocate_Lic_" + idSour + "_" + idDest + ".zip");
        try {
            relocator.relocateId("Lic", idSour, idDest, outFile);
            throw new XError("Нельзя перемещать в занятую");
        } catch (Exception e) {
            if (!e.getMessage().contains("Error relocateId: dest id already exists:")) {
                throw e;
            }
        }

        //
        st = db.loadSql(sql);
        UtData.outTable(st);
    }


    @Test
    public void test_relocate_range() throws Exception {
        String tableNames = "Lic,LicDocTip,LicDocVid,Ulz,UlzTip,Region,RegionTip";
        //String tableNames = "LicDocTip,LicDocVid";
        String[] tableNamesArr = tableNames.split(",");
        //
        long minPkValue = 1000000;
        long maxPkValue = 100000000000L;
        long normalPkValue = 1000;
        //
        String tempDirName = "temp/relocate/";
        UtFile.cleanDir(tempDirName);

        //
        IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain();
        JdxRecRelocator relocator = new JdxRecRelocator(db, struct, dataSerializer);


        //
        System.out.println("===========================");
        System.out.println("До переноса:");
        for (String tableName : tableNamesArr) {
            String sql = "select * from " + tableName + " order by id desc";
            DataStore st = db.loadSql(sql);
            UtData.outTable(st, 3);
        }


        // Создадим себе проблему
        System.out.println();
        System.out.println("===========================");
        System.out.println("Перенесем некоторые записи на id > " + minPkValue);
        for (String tableName : tableNamesArr) {
            long idSour = db.loadSql("select min(id) id from " + tableName + " where id <> 0").getCurRec().getValueLong("id");
            long idDest = minPkValue + idSour;
            relocator.relocateId(tableName, idSour, idDest, new File(tempDirName + "1.zip"));
        }
        //
        for (String tableName : tableNamesArr) {
            String sql = "select * from " + tableName + " order by id desc";
            DataStore st = db.loadSql(sql);
            UtData.outTable(st, 3);
        }


        // Героически ее решим
        System.out.println();
        System.out.println("===========================");
        System.out.println("Вернем записи на id > " + normalPkValue);
        for (String tableName : tableNamesArr) {
            List<String> idsSour = new ArrayList<>();
            List<String> idsDest = new ArrayList<>();
            long idDestFrom = db.loadSql("select min(id) id from " + tableName + " where id >= " + normalPkValue + " and id <= " + minPkValue).getCurRec().getValueLong("id");
            if (idDestFrom == 0) {
                idDestFrom = normalPkValue;
            }
            try {
                relocator.rec_relocate_paramsRange(tableName, minPkValue, maxPkValue, idDestFrom, idsSour, idsDest);
            } catch (Exception e) {
                if (!e.getMessage().contains("уже есть записи")) {
                    throw e;
                }
            }

            //
            idsSour = new ArrayList<>();
            idsDest = new ArrayList<>();
            idDestFrom = db.loadSql("select max(id) id from " + tableName + " where id >= " + normalPkValue + " and id <= " + minPkValue).getCurRec().getValueLong("id");
            if (idDestFrom == 0) {
                idDestFrom = normalPkValue;
            }
            relocator.rec_relocate_paramsRange(tableName, minPkValue, maxPkValue, idDestFrom + 1, idsSour, idsDest);

            //
            relocator.relocateIdList(tableName, idsSour, idsDest, tempDirName);
        }


        //
        System.out.println();
        System.out.println("===========================");
        System.out.println("Стало:");
        for (String tableName : tableNamesArr) {
            String sql = "select * from " + tableName + " order by id desc";
            DataStore st = db.loadSql(sql);
            UtData.outTable(st, 3);
        }

        //
        for (String tableName : tableNamesArr) {
            String sql = "select * from " + tableName + " where id >= " + minPkValue + " order by id desc";
            DataStore st = db.loadSql(sql);
            assertEquals(0, st.size());
        }
    }

}
