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
        JdxReplicaWriterXml_Test writerXml_test = new JdxReplicaWriterXml_Test();
        writerXml_test.setUp();
        writerXml_test.test_getFull();
        //
        System.out.println("==================");

        //
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("../_test-data/ws_002/tmp/000000001-src.zip"));

        // Откроем Zip-файл
        InputStream inputStream = UtRepl.getReplicaInputStream(replica);

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(inputStream);
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
                System.out.println("  " + rec);

                //
                rec = reader.nextRec();
            }

            tableName = reader.nextTable();
        }

        // Закроем читателя Zip-файла
        inputStream.close();
    }

    @Test
    public void test_big() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.setFile(new File("../_test-data/000000001-big.zip"));

        // Откроем Zip-файл
        InputStream inputStream = UtRepl.getReplicaInputStream(replica);

        //
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(inputStream);
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

        // Закроем читателя Zip-файла
        inputStream.close();
    }

}
