package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.data_serializer.*;
import org.junit.*;

import java.io.*;

public class JdxRecRemover_Test extends JdxRecRelocator_Test {

    @Test
    public void test_delete() throws Exception {
        //String tableName = "Ws_List";
        String tableName = "Lic";

        //
        String sql000 = "select * from " + tableName + " where id <> 0 order by id";
        DataStore st000 = db.loadSql(sql000);
        long idSour = st000.get(0).getValueLong("id");
        //
        String sql001 = "select * from " + tableName + " where id <> 0 and id = " + idSour;
        //String values1 = db.loadSql(sqlCheck0).get(1).getValues().toString();

        //
        String sql0 = "select id, NameF, NameI, NameO, BornDt, DocDt from Lic where id <> 0 order by id";
        String sql1 = "select id as PawnChit, ChitNo, ChitDt, Lic, ws_list from PawnChit where id <> 0 order by id";
        String sql2 = "select id, PawnChit, SubjectNo, Info from PawnChitSubject where id <> 0 order by PawnChit, SubjectNo";
        String sql3 = "select PawnChitSubject, count(*) from ValPawnChitSubject where id <> 0 group by PawnChitSubject order by PawnChitSubject";
        String sqlCheck0 = "select * from Lic order by id";
        String sqlCheck1 = "select * from PawnChit order by id";
        String sqlCheck2 = "select * from PawnChitSubject order by id";
        String sqlCheck3 = "select * from ValPawnChitSubject order by id";

        //
        System.out.println("Было");
        DataStore st001 = db.loadSql(sql001);
        DataStore st0 = db.loadSql(sql0);
        DataStore st1 = db.loadSql(sql1);
        DataStore st2 = db.loadSql(sql2);
        DataStore st3 = db.loadSql(sql3);
        UtData.outTable(st001);
        System.out.println();
        UtData.outTable(st0);
        UtData.outTable(st1);
        UtData.outTable(st2);
        UtData.outTable(st3);


        File outFile = new File("temp/remove_" + tableName + "_" + idSour + ".zip");


        //
        System.out.println();
        System.out.println("Удаление, table: " + tableName + ", id: " + idSour);
        UtData.outTable(st000);
        System.out.println();


        //
        IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain();
        JdxRecRemover remover = new JdxRecRemover(db, struct, dataSerializer);
        //
        remover.removeId(tableName, idSour, outFile);


        //
        System.out.println();
        System.out.println("------------------------------------");
        System.out.println();
        System.out.println("Стало");
        st001 = db.loadSql(sql001);
        st0 = db.loadSql(sql0);
        st1 = db.loadSql(sql1);
        st2 = db.loadSql(sql2);
        st3 = db.loadSql(sql3);
        UtData.outTable(st001);
        System.out.println();
        UtData.outTable(st0);
        UtData.outTable(st1);
        UtData.outTable(st2);
        UtData.outTable(st3);


        //
        assertEquals("Запись не исчезла: ", 0, db.loadSql(sql001).getCurRec().getValueLong("id"));


        //
        //String values2 = db.loadSql(sqlCheck0).get(1).getValues().toString();
        //assertEquals("Записи не одинаковые: ", values1, values2);
    }

}
