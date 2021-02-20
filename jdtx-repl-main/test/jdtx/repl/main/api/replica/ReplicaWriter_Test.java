package jdtx.repl.main.api.replica;

import jandcode.utils.test.*;
import org.apache.commons.io.*;
import org.joda.time.*;
import org.junit.*;

import java.io.*;
import java.nio.charset.*;

public class ReplicaWriter_Test extends UtilsTestCase {

    @Test
    public void test_1() throws Exception {
        IReplica replica = new ReplicaFile();

        //
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replica.getInfo().setWsId(12);
        replica.getInfo().setAge(12345);
        replica.getInfo().setDtFrom(new DateTime("2000-01-31T00:30:00"));
        replica.getInfo().setDtTo(new DateTime("2000-01-31T00:40:59"));


        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();


        // Начинаем писать просто файл test_1.txt
        OutputStream stream = replicaWriter.newFileOpen("test_1.txt");
        //
        String s1 = "Текстовая строка в файле test_1.txt";
        stream.write(s1.getBytes(StandardCharsets.UTF_8));


        // Начинаем писать xml-файл с данными реплики
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument();
        //
        xmlWriter.startTable("ABC");
        xmlWriter.appendRec();
        xmlWriter.setRecValue("X", 1);
        xmlWriter.setRecValue("Y", "YYY");
        xmlWriter.setRecValue("Z", new DateTime("2000-01-31T22:30:00"));
        //
        xmlWriter.startTable("DEF");
        xmlWriter.appendRec();
        xmlWriter.setRecValue("A", 222);
        xmlWriter.setRecValue("B", new DateTime());
        xmlWriter.setRecValue("C", "Qwerty Йцукен");
        //
        xmlWriter.closeDocument();

        // Начинаем писать просто файл test_2.txt
        stream = replicaWriter.newFileOpen("test_2.txt");
        //
        String s2 = "Текстовая строка в файле test_2.txt";
        stream.write(s2.getBytes(StandardCharsets.UTF_8));


        // Начинаем писать еще xml-файл с данными реплики (dat_2.xml)
        stream = replicaWriter.newFileOpen("dat_2.xml");
        xmlWriter = new JdxReplicaWriterXml(stream);
        //
        xmlWriter.startDocument();
        //
        xmlWriter.startTable("XYZ");
        xmlWriter.appendRec();
        xmlWriter.setRecValue("A", "A");
        xmlWriter.setRecValue("B", "B");
        xmlWriter.setRecValue("C", "C");
        //
        xmlWriter.closeDocument();


        // Закрываем writer
        replicaWriter.replicaFileClose();


        // На посмотреть
        File testFile = new File("../_test-data/ReplicaWriter_Test.zip");
        testFile.delete();
        FileUtils.moveFile(replica.getFile(), testFile);
    }


}
