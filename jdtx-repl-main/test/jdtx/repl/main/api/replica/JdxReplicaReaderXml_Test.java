package jdtx.repl.main.api.replica;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class JdxReplicaReaderXml_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_read_1() throws Exception {
        // Начинаем читать xml-файл с данными
        File testFile_1 = new File("../_test-data/test_1.xml");
        FileInputStream stream = new FileInputStream(testFile_1);
        JdxReplicaReaderXml xmlReader = new JdxReplicaReaderXml(stream);

        //
        IJdxField fieldString = struct.getTable("LicDocTip").getField("Name");

        //
        String tableName = xmlReader.nextTable();
        while (tableName != null) {
            System.out.println("tableName: " + tableName);

            Map<String, String> values = xmlReader.nextRec();
            while (values != null) {
                for (String key : values.keySet()) {
                    String value = values.get(key);
                    System.out.println("  " + key + " = [" + value + "] " + " = " + UtXml.strToValue(value, fieldString));
                }
                System.out.println("---");
                //
                values = xmlReader.nextRec();
            }

            //
            System.out.println();

            //
            tableName = xmlReader.nextTable();
        }

        //
        xmlReader.close();
    }

    @Test
    public void test_read_2() throws Exception {
        // Начинаем читать xml-файл с данными
        File testFile_1 = new File("../_test-data/test_2.xml");
        FileInputStream stream = new FileInputStream(testFile_1);
        JdxReplicaReaderXml xmlReader = new JdxReplicaReaderXml(stream);

        //
        IJdxField fieldBlob = struct.getTable("PawnChitDat").getField("Dat");

        //
        String tableName = xmlReader.nextTable();
        while (tableName != null) {
            System.out.println("tableName: " + tableName);

            Map<String, String> values = xmlReader.nextRec();
            while (values != null) {
                for (String key : values.keySet()) {
                    String value = values.get(key);
                    System.out.println("  " + key + " = " + value.substring(0, Math.min(value.length(), 50)));
                }
                System.out.println("---");
                FileUtils.writeByteArrayToFile(new File("../_test-data/" + testFile_1.getName() + "_" + values.get("NAME")), (byte[]) UtXml.strToValue(values.get("XYZ"), fieldBlob));
                //
                values = xmlReader.nextRec();
            }

            //
            System.out.println();

            //
            tableName = xmlReader.nextTable();
        }

        //
        xmlReader.close();
    }


    @Test
    public void test_readReplicaInfo() throws Exception {
        // Готовим реплику от ws2
        File fileSnapshot = new File("../_test-data/_test-data_srv/srv/que_common/000000/000000001.zip");
        IReplica replicaSnapshot = new ReplicaFile();
        replicaSnapshot.setFile(fileSnapshot);

        // Проверяем чтение заголовков
        File file = new File(replicaSnapshot.getFile().getAbsolutePath());
        IReplica replica = new ReplicaFile();
        replica.setFile(file);

        //
        JdxReplicaReaderXml.readReplicaInfo(replica);
        System.out.println("replica: " + file);
        System.out.println("replica.wsId = " + replica.getInfo().getWsId());
        System.out.println("replica.age = " + replica.getInfo().getAge());
        System.out.println("replica.replicaType = " + replica.getInfo().getReplicaType());
    }


}
