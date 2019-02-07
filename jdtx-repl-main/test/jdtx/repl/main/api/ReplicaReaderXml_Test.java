package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class ReplicaReaderXml_Test extends DbmTestCase {

    @Test
    public void test_1() throws Exception {
        IReplica replica = new Replica();
        replica.setFile(new File("temp/csv.xml"));

        //
        ReplicaReaderXml reader = new ReplicaReaderXml(replica);
        System.out.println("DbId = " + reader.getDbId());

        //
        String tableName = reader.nextTable();
        while (tableName != null) {
            System.out.println("table [" + tableName + "]");

            // Перебираем записи
            Map rec = reader.nextRec();
            while (rec != null) {
                System.out.println("rec=" + rec);

                //
                rec = reader.nextRec();
            }

            tableName = reader.nextTable();
        }
    }

}
