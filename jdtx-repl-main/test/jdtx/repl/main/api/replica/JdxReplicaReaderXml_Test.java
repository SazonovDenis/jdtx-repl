package jdtx.repl.main.api.replica;

import jandcode.dbm.test.*;
import jdtx.repl.main.api.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class JdxReplicaReaderXml_Test extends DbmTestCase {


    @Test
    public void test_JdxReplicaReader_big() throws Exception {
        //
        IReplica replicaSnapshot = new ReplicaFile();

        // Читаем и печатаем реплику
        replicaSnapshot.setFile(new File("D:/t/000000082.zip"));
        readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000083.zip"));
        readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000084.zip"));
        readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000085.zip"));
        readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000086.zip"));
        readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000087.zip"));
        readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000088.zip"));
        readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000089.zip"));
        readPrintReplica(replicaSnapshot);
    }


    @Test
    public void test_JdxReplicaReader() throws Exception {
        // Готовим реплику от ws2
        UtData_Test writerXml_test = new UtData_Test();
        writerXml_test.setUp();
        IReplica replicaSnapshot = writerXml_test.createReplicaSnapshot_Ulz_ws2();

        // Читаем и печатаем реплику
        readPrintReplica(replicaSnapshot);
    }


    @Test
    public void test_readReplicaInfo() throws Exception {
        // Готовим реплику от ws2
        UtData_Test writerXml_test = new UtData_Test();
        writerXml_test.setUp();
        IReplica replicaSnapshot = writerXml_test.createReplicaSnapshot_Ulz_ws2();

        // Проверяем чтение заголовков
        File f = new File(replicaSnapshot.getFile().getAbsolutePath());
        IReplica replica = new ReplicaFile();
        replica.setFile(f);

        //
        JdxReplicaReaderXml.readReplicaInfo(replica);
        System.out.println("replica: " + f);
        System.out.println("replica.wsId = " + replica.getInfo().getWsId());
        System.out.println("replica.age = " + replica.getInfo().getAge());
        System.out.println("replica.replicaType = " + replica.getInfo().getReplicaType());
    }


    private void readPrintReplica(IReplica replicaSnapshot) throws Exception {
        // Откроем Zip-файл реплики
        IReplica replica = new ReplicaFile();
        replica.setFile(new File(replicaSnapshot.getFile().getAbsolutePath()));
        InputStream inputStream = UtRepl.getReplicaInputStream(replica);

        // Читаем заголовки
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(inputStream);
        System.out.println("WsId = " + reader.getWsId());
        System.out.println("Age = " + reader.getAge());
        System.out.println("ReplicaType = " + reader.getReplicaType());

        // Читаем данные
        String tableName = reader.nextTable();
        while (tableName != null) {
            System.out.println("table [" + tableName + "]");

            //
            long count = 0;

            // Перебираем записи
            Map rec = reader.nextRec();
            StringBuffer sb = new StringBuffer();
            while (rec != null) {
                count++;
                //
                sb.setLength(0);
                Set<Map.Entry> es = rec.entrySet();
                for (Map.Entry x : es) {
                    if (sb.length() != 0) {
                        sb.append(", ");
                    }
                    String val = String.valueOf(x.getValue());
                    if (val.length() > 30) {
                        val = val.substring(0, 20) + "...";
                    }
                    sb.append(x.getKey() + ": " + val);
                }
                System.out.println("  " + sb);

                //
                rec = reader.nextRec();
            }

            //
            System.out.println(tableName + ".count: " + count);

            //
            tableName = reader.nextTable();
        }

        // Закроем читателя Zip-файла
        inputStream.close();
    }


}
