package jdtx.repl.main.api;

import org.junit.*;

import java.util.*;

/**
 *
 */
public class UtDbComparer_Data_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_getDbDataCrc() throws Exception {
        Map<String, Map<String, String>> dbCrcSrv = getWsDbDataCrc(db);

        //
        System.out.println("dbCrcSrv.size: " + dbCrcSrv.size());
        System.out.println("  " + dbCrcSrv.keySet());
    }


    @Test
    public void test_compare() throws Exception {
        System.out.println("dbSrv");
        Map<String, Map<String, String>> dbCrcSrv = getWsDbDataCrc(db);
        System.out.println();

        System.out.println("db1");
        Map<String, Map<String, String>> dbCrcWs2 = getWsDbDataCrc(db2);
        System.out.println();

        System.out.println("db3");
        Map<String, Map<String, String>> dbCrcWs3 = getWsDbDataCrc(db3);
        System.out.println();

        //
        System.out.println("dbSrv vs dbWs2");
        UtDbComparer.compareDbDataCrc(dbCrcSrv, dbCrcWs2);
        System.out.println();

        //
        System.out.println("dbWs2 vs dbWs3");
        UtDbComparer.compareDbDataCrc(dbCrcWs2, dbCrcWs3);
        System.out.println();
    }


}
