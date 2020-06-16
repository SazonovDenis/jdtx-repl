package jdtx.repl.main.ext;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtRecMerge_Test extends DbPrepareEtalon_Test {

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
        String namesStr = "NameF,NameI,DocNo";
        //String namesStr = "RNN";
        //String namesStr = "LICDOCVID";
        //String namesStr = "DocNo";
        //
        String[] fieldNames = namesStr.split(",");

        //
        UtRecMerge utRecMerge = new UtRecMerge(db);
        List<UtRecMerge.UtRecMergeRes> resList = utRecMerge.recordAnalys(tableName, fieldNames);

        //
        for (UtRecMerge.UtRecMergeRes res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.store, 10);
        }

        //
        System.out.println();
        System.out.println("Match count: " + resList.size());
    }

}
