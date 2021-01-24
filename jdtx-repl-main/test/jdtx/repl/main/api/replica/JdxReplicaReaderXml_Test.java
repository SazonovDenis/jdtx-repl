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
        UtData_Test.readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000083.zip"));
        UtData_Test.readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000084.zip"));
        UtData_Test.readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000085.zip"));
        UtData_Test.readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000086.zip"));
        UtData_Test.readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000087.zip"));
        UtData_Test.readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000088.zip"));
        UtData_Test.readPrintReplica(replicaSnapshot);
        //
        replicaSnapshot.setFile(new File("D:/t/000000089.zip"));
        UtData_Test.readPrintReplica(replicaSnapshot);
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


}
