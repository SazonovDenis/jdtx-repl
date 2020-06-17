package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtRecMerge_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_1() throws Exception {
        InputStream inputStream = new FileInputStream("test/jdtx/repl/main/ext/UtRecMergeTest.xml");
        UtRecMergeReaderXml reader = new UtRecMergeReaderXml(inputStream);
        Map map;
        map = reader.nextRec();
        while (map != null) {
            System.out.println(map);
            map = reader.nextRec();
        }
    }

    @Test
    public void test_2() throws Exception {
        db.connect();

        //
        //String tableName = "Ulz";
        //String namesStr = "Name,UlzTip";
        String tableName = "Lic";
        //
        //String namesStr = "NameF,NameI,DocNo";
        String namesStr = "RNN";
        //String namesStr = "LICDOCVID";
        //String namesStr = "DocNo";
        //
        String[] fieldNames = namesStr.split(",");

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        Collection<UtRecDuplicate> resList = utRecMerge.loadTableDuplicates(tableName, fieldNames);

        //
        for (UtRecDuplicate res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
        }

        //
        System.out.println();
        System.out.println("Match count: " + resList.size());
    }

}
