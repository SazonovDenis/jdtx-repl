package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import jdtx.repl.main.api.data_serializer.*;
import org.junit.*;

import java.io.*;

public class JdxRecRemover_Test extends JdxRecRelocator_Test {

    //String tableName = "Ws_List";
    String tableName = "Lic";
    long tableId = 1357;

    @Test
    public void test_removeRecCascade() throws Exception {
        long idSour = getIdSour();
        //
        String sql001 = "select * from " + tableName + " where id <> 0 and id = " + idSour;
        String sql0 = "select id, NameF, NameI, NameO, BornDt, DocDt from Lic where id <> 0 order by id";
        String sql1 = "select id as PawnChit, ChitNo, ChitDt, Lic, ws_list from PawnChit where id <> 0 order by id";
        String sql2 = "select id, PawnChit, SubjectNo, Info from PawnChitSubject where id <> 0 order by PawnChit, SubjectNo";
        String sql3 = "select PawnChitSubject, count(*) from ValPawnChitSubject where id <> 0 group by PawnChitSubject order by PawnChitSubject";

        //
        System.out.println("Было");
        DataStore st001_1 = db.loadSql(sql001);
        DataStore st0_1 = db.loadSql(sql0);
        DataStore st1_1 = db.loadSql(sql1);
        DataStore st2_1 = db.loadSql(sql2);
        DataStore st3_1 = db.loadSql(sql3);
        UtData.outTable(st001_1);
        System.out.println();
        UtData.outTable(st0_1);
        UtData.outTable(st1_1);
        UtData.outTable(st2_1);
        UtData.outTable(st3_1);

        //
        String fileName1 = "temp/remove_" + tableName + "_" + idSour + ".0.zip";
        String fileName2 = "temp/remove_" + tableName + "_" + idSour + ".1.zip";
        String fileName3 = "temp/remove_" + tableName + "_" + idSour + ".2.zip";

        //
        OutTableSaver svr_st001 = new OutTableSaver(st001_1);
        OutTableSaver svr_st0 = new OutTableSaver(st0_1);
        OutTableSaver svr_st1 = new OutTableSaver(st1_1);
        OutTableSaver svr_st2 = new OutTableSaver(st2_1);
        OutTableSaver svr_st3 = new OutTableSaver(st3_1);
        UtFile.saveString(svr_st001.save().toString() + "\n\n" + svr_st0.save().toString() + "\n\n" + svr_st1.save().toString() + "\n\n" + svr_st2.save().toString() + "\n\n" + svr_st3.save().toString(), new File(fileName1));


        //
        System.out.println();
        System.out.println("Удаление, table: " + tableName + ", id: " + idSour);
        System.out.println();


        //
        IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain();
        JdxRecRemover remover = new JdxRecRemover(db, struct, dataSerializer);
        //
        File outFile = new File("temp/remove_" + tableName + "_" + idSour + ".zip");
        remover.removeRecCascade(tableName, idSour, outFile);


        //
        System.out.println();
        System.out.println("------------------------------------");
        System.out.println();
        System.out.println("Стало");
        DataStore st001_2 = db.loadSql(sql001);
        DataStore st0_2 = db.loadSql(sql0);
        DataStore st1_2 = db.loadSql(sql1);
        DataStore st2_2 = db.loadSql(sql2);
        DataStore st3_2 = db.loadSql(sql3);
        UtData.outTable(st001_2);
        System.out.println();
        UtData.outTable(st0_2);
        UtData.outTable(st1_2);
        UtData.outTable(st2_2);
        UtData.outTable(st3_2);

        //
        svr_st001 = new OutTableSaver(st001_2);
        svr_st0 = new OutTableSaver(st0_2);
        svr_st1 = new OutTableSaver(st1_2);
        svr_st2 = new OutTableSaver(st2_2);
        svr_st3 = new OutTableSaver(st3_2);
        UtFile.saveString(svr_st001.save().toString() + "\n\n" + svr_st0.save().toString() + "\n\n" + svr_st1.save().toString() + "\n\n" + svr_st2.save().toString() + "\n\n" + svr_st3.save().toString(), new File(fileName2));


        // Запись исчезла?
        assertEquals("Запись не исчезла: ", 0, db.loadSql(sql001).getCurRec().getValueLong("id"));
        assertNotSame("Запись не исчезла: ", st001_1.size(), st001_2.size());
        if (!tableName.equalsIgnoreCase("Ws_List")) {
            assertNotSame("Запись не исчезла: ", st0_1.size(), st0_2.size());
        }
        assertNotSame("Запись не исчезла: ", st1_1.size(), st1_2.size());
        assertNotSame("Запись не исчезла: ", st2_1.size(), st2_2.size());
        assertNotSame("Запись не исчезла: ", st3_1.size(), st3_2.size());


        // Отменяем удаление
        System.out.println();
        System.out.println("Отменяем удаление");
        System.out.println();
        // 
        JdxRecMerger recMerger = new JdxRecMerger(db, struct, dataSerializer);
        recMerger.revertExec(outFile);


        //
        System.out.println();
        System.out.println("------------------------------------");
        System.out.println();
        System.out.println("После восстановления");
        DataStore st001_3 = db.loadSql(sql001);
        DataStore st0_3 = db.loadSql(sql0);
        DataStore st1_3 = db.loadSql(sql1);
        DataStore st2_3 = db.loadSql(sql2);
        DataStore st3_3 = db.loadSql(sql3);
        UtData.outTable(st001_3);
        System.out.println();
        UtData.outTable(st0_3);
        UtData.outTable(st1_3);
        UtData.outTable(st2_3);
        UtData.outTable(st3_3);

        //
        svr_st001 = new OutTableSaver(st001_3);
        svr_st0 = new OutTableSaver(st0_3);
        svr_st1 = new OutTableSaver(st1_3);
        svr_st2 = new OutTableSaver(st2_3);
        svr_st3 = new OutTableSaver(st3_3);
        UtFile.saveString(svr_st001.save().toString() + "\n\n" + svr_st0.save().toString() + "\n\n" + svr_st1.save().toString() + "\n\n" + svr_st2.save().toString() + "\n\n" + svr_st3.save().toString(), new File(fileName3));

        //
        startCmpDb(fileName1, fileName2, fileName3);


        // Запись появилась?
        assertEquals("Запись не вернулась: ", idSour, db.loadSql(sql001).getCurRec().getValueLong("id"));
        assertEquals("Запись не вернулась: ", st001_1.size(), st001_3.size());
        assertEquals("Запись не вернулась: ", st0_1.size(), st0_3.size());
        assertEquals("Запись не вернулась: ", st1_1.size(), st1_3.size());
        assertEquals("Запись не вернулась: ", st2_1.size(), st2_3.size());
        assertEquals("Запись не вернулась: ", st3_1.size(), st3_3.size());
    }

    void startCmpDb(String fileName1, String fileName2, String fileName3) throws Exception {
        String batContent = "diff " + new File(fileName1).getAbsolutePath() + " " + new File(fileName2).getAbsolutePath() + " " + new File(fileName3).getAbsolutePath();
        File batFile = new File("temp/cmp_db.bat");
        UtFile.saveString(batContent, batFile);
        ProcessBuilder processBuilder = new ProcessBuilder("cmd", "/C", batFile.getAbsolutePath());
        processBuilder.directory(batFile.getParentFile());
        Process process = processBuilder.start();
        process.waitFor();
    }

    @Test
    public void printDb() throws Exception {
        long idSour = getIdSour();
        //
        String sql001 = "select * from " + tableName + " where id <> 0 and id = " + idSour;
        String sql0 = "select id, NameF, NameI, NameO, BornDt, DocDt from Lic where id <> 0 order by id";
        String sql1 = "select id as PawnChit, ChitNo, ChitDt, Lic, ws_list from PawnChit where id <> 0 order by id";
        String sql2 = "select id, PawnChit, SubjectNo, Info from PawnChitSubject where id <> 0 order by PawnChit, SubjectNo";
        String sql3 = "select PawnChitSubject, count(*) from ValPawnChitSubject where id <> 0 group by PawnChitSubject order by PawnChitSubject";
        //
        DataStore st001_3 = db.loadSql(sql001);
        DataStore st0_3 = db.loadSql(sql0);
        DataStore st1_3 = db.loadSql(sql1);
        DataStore st2_3 = db.loadSql(sql2);
        DataStore st3_3 = db.loadSql(sql3);
        UtData.outTable(st001_3);
        System.out.println();
        UtData.outTable(st0_3);
        UtData.outTable(st1_3);
        UtData.outTable(st2_3);
        UtData.outTable(st3_3);
    }

    private long getIdSour() throws Exception {
/*
        String sql000 = "select * from " + tableName + " where id <> 0 order by id";
        DataStore st000 = db.loadSql(sql000);
        long idSour = st000.get(0).getValueLong("id");
        return idSour;
*/
        return tableId;
    }

}
