package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class JdxReplicaReaderXml_Test extends DbmTestCase {

    @Test
    public void test_1() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("../_test-data/~tmp_csv.xml"));

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(replica.getFile());
        System.out.println("WsId = " + reader.getWsId());
        System.out.println("Age = " + reader.getAge());
        System.out.println("ReplicaType = " + reader.getReplicaType());

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

    @Test
    public void test_big() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("Z:/jdtx-repl/temp/000000001.xml"));

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(replica.getFile());
        System.out.println("WsId = " + reader.getWsId());
        System.out.println("Age = " + reader.getAge());
        System.out.println("ReplicaType = " + reader.getReplicaType());

        //
        String tableName = reader.nextTable();
        while (tableName != null) {
            System.out.println("table [" + tableName + "]");

            // Перебираем записи
            Map rec = reader.nextRec();
            while (rec != null) {
                if (tableName.equals("SUBJECTTIP")) {
                    System.out.println("rec=" + rec);
                }

                //
                rec = reader.nextRec();
            }

            tableName = reader.nextTable();
        }
    }

}
