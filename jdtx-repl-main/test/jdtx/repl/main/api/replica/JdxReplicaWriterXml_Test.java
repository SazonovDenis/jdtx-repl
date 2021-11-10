package jdtx.repl.main.api.replica;

import jandcode.utils.test.*;
import org.apache.commons.io.*;
import org.joda.time.*;
import org.junit.*;

import java.io.*;

public class JdxReplicaWriterXml_Test extends UtilsTestCase {


    @Test
    public void test() throws Exception {
        File testFile_1 = new File("../_test-data/test_1.xml");
        File testFile_2 = new File("../_test-data/test_2.xml");
        testFile_1.delete();
        testFile_2.delete();

        // Начинаем писать просто файл test_1.xml
        OutputStream stream = new FileOutputStream(testFile_1);
        JdxReplicaWriterXml xmlWriter = new JdxReplicaWriterXml(stream);
        xmlWriter.startDocument();
        xmlWriter.writeReplicaInfo(new ReplicaInfo());
        //
        xmlWriter.startTable("XYZ");
        xmlWriter.appendRec();
        xmlWriter.writeRecValue("A", "value \n\rA");
        xmlWriter.writeRecValue("B", "value \tB");
        xmlWriter.writeRecValue("C", "value \\/\\C");
        xmlWriter.writeRecValue("D", "value =D ==D");
        //
        xmlWriter.closeDocument();
        stream.close();

        // Начинаем писать просто xml-файл test_2.xml
        stream = new FileOutputStream(testFile_2);
        xmlWriter = new JdxReplicaWriterXml(stream);
        xmlWriter.startDocument();
        xmlWriter.writeReplicaInfo(new ReplicaInfo());
        //
        xmlWriter.startTable("ABC");
        xmlWriter.appendRec();
        xmlWriter.writeRecValue("X", 1);
        xmlWriter.writeRecValue("Y", "YYY");
        xmlWriter.writeRecValue("Z", new DateTime("2000-01-31T22:30:00"));
        xmlWriter.writeRecValue("NAME", "test.dat");
        xmlWriter.writeRecValue("XYZ", new byte[]{0x1E, 0x12, 0x46, 0x23, (byte) 0xFF, 0x6A, 0x12, 0x34, 0x7F, 0x0});
        //
        xmlWriter.startTable("DEF");
        xmlWriter.appendRec();
        xmlWriter.writeRecValue("A", 123456L);
        xmlWriter.writeRecValue("B", new DateTime());
        xmlWriter.writeRecValue("C", "Qwerty Йцукен");
        xmlWriter.writeRecValue("NAME", "test.png");
        xmlWriter.writeRecValue("XYZ", FileUtils.readFileToByteArray(new File("test/jdtx/repl/main/api/replica/JdxReplicaWriterXml.png")));
        //
        xmlWriter.appendRec();
        xmlWriter.writeRecValue("A", 12345678901234L);
        xmlWriter.writeRecValue("B", new DateTime());
        xmlWriter.writeRecValue("NAME", "test.jpg");
        xmlWriter.writeRecValue("XYZ", FileUtils.readFileToByteArray(new File("test/jdtx/repl/main/api/replica/JdxReplicaWriterXml.jpg")));
        //
        xmlWriter.closeDocument();
        stream.close();
    }


}
