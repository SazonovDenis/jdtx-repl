package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 * ��������/�������� �������������� ��������
 */
public class UtRepl_CreateRepl_Test extends Repl_Test_Custom {


    @Test
    public void test_db() throws Exception {
        DataStore st = dbm.getDb().loadSql("select id, orgName from dbInfo");
        dbm.outTable(st);
    }

    @Test
    public void test_dropReplication() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());
        utr.dropReplication();
    }

    @Test
    public void test_createReplication() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());
        //utr.dropReplication();

        //
        JdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(dbm.getDb());
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();

        //
        IJdxDbStruct struct_1 = reader.readDbStruct(false);
        struct_rw.write(struct_1, "temp/dbStruct_1.xml");

        //
        utr.createReplication();
        //
        utr.dropReplication();

        //
        IJdxDbStruct struct_2 = reader.readDbStruct(false);
        struct_rw.write(struct_2, "temp/dbStruct_2.xml");

        // �������� ����������
        utt.compareStruct(struct_1, struct_2);
    }


}
