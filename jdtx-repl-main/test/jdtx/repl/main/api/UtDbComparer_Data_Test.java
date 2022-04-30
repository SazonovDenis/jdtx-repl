package jdtx.repl.main.api;

import org.junit.*;

import java.util.*;

/**
 *
 */
public class UtDbComparer_Data_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_getDbDataCrc() throws Exception {
        Map<String, Map<String, String>> dbCrcSrv = loadWsDbDataCrc(db);

        //
        System.out.println("dbCrcSrv.size: " + dbCrcSrv.size());
        System.out.println("  " + dbCrcSrv.keySet());
    }


    @Test
    public void test_compare() throws Exception {
        System.out.println("dbSrv");
        Map<String, Map<String, String>> dbCrcSrv = loadWsDbDataCrc(db);
        System.out.println();

        System.out.println("db2");
        Map<String, Map<String, String>> dbCrcWs2 = loadWsDbDataCrc(db2);
        System.out.println();

        System.out.println("db3");
        Map<String, Map<String, String>> dbCrcWs3 = loadWsDbDataCrc(db3);
        System.out.println();

        //
        Map<String, Set<String>> diffCrc = new HashMap<>();
        Map<String, Set<String>> diffNewIn1 = new HashMap<>();
        Map<String, Set<String>> diffNewIn2 = new HashMap<>();

        //
        System.out.println("dbSrv vs dbWs2");
        UtDbComparer.compareDbDataCrc(dbCrcSrv, dbCrcWs2, diffCrc, diffNewIn1, diffNewIn2);
        System.out.println();

        //
        System.out.println("dbSrv vs dbWs3");
        UtDbComparer.compareDbDataCrc(dbCrcSrv, dbCrcWs3, diffCrc, diffNewIn1, diffNewIn2);
        System.out.println();

        //
        System.out.println("dbWs2 vs dbWs3");
        UtDbComparer.compareDbDataCrc(dbCrcWs2, dbCrcWs3, diffCrc, diffNewIn1, diffNewIn2);
        System.out.println();
    }


}
