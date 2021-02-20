package jdtx.repl.main.api.replica;


import jandcode.utils.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.zip.*;

/**
 * Утилитный класс для формирования файла реплики.
 * Инкапсулирует логику формирования файла, врапер над всякими утилитными классами.
 */
public class UtReplicaWriter {

    //
    private OutputStream outputStream = null;
    private ZipOutputStream zipOutputStream = null;
    private JdxReplicaWriterXml writerXml = null;
    private IReplica replica = null;

    //
    protected static Log log = LogFactory.getLog("jdtx.UtReplicaWriter");


    public UtReplicaWriter(IReplica replica){
        this.replica = replica;
    }

    /**
     * Начинает формировать физический файл для реплики
     */
    public void replicaFileStart() throws Exception {
        // Файл
        File outFile = createTempFileReplica(replica);
        replica.setFile(outFile);

        //
        outputStream = new FileOutputStream(outFile);

        // Формируем Zip-архив
        zipOutputStream = new ZipOutputStream(outputStream);

        // Файл "dat.info" внутри Zip-архива (заголовок с информацией о реплике, сериализация IReplicaInfo)
        ZipEntry zipEntryHead = new ZipEntry("dat.info");
        zipOutputStream.putNextEntry(zipEntryHead);
        String json = replica.getInfo().toString();
        zipOutputStream.write(json.getBytes("utf-8"));
        zipOutputStream.closeEntry();
    }

    /**
     * Заканчивает формировать физический файл для реплики
     */
    public void replicaFileClose() throws Exception {
        // Заканчиваем запись в XML-файл
        if (writerXml != null) {
            writerXml.closeDocument();
            writerXml.close();
        }

        // Заканчиваем запись в в zip-архив
        zipOutputStream.closeEntry();
        zipOutputStream.finish();
        zipOutputStream.close();

        // Закрываем файл
        outputStream.close();
    }

    /**
     * Добавляет произвольный файл внутри формируемого Zip-архива.
     * <p>
     * Симметричныый метод newFileClose() - пока не нужен, и поэтому отсутствует.
     * Не нужен, т.к. вызов zipOutputStream.closeEntry() сейчас сделан прямо внутри replicaFileClose()
     */
    public OutputStream newFileOpen(String fileName) throws Exception {
        ZipEntry zipEntry = new ZipEntry(fileName);
        //
        zipOutputStream.putNextEntry(zipEntry);
        //
        return zipOutputStream;
    }

    /**
     * Добавляет xml-файл с репликой ("dat.xml") внутри формируемого Zip-архива.
     * Создет writer для данных реплики, возвращает JdxReplicaWriterXml.
     * <p>
     * Симметричныый метод replicaWriterCloseDocument - пока не нужен, и поэтому отсутствует.
     * А не нужен он потому, что вызов writerXml.closeDocument() после writerXml.startDocument()
     * сейчас сделан прямо внутри jdtx.repl.main.api.replica.UtReplicaWriter#replicaFileClose()
     */
    public JdxReplicaWriterXml replicaWriterStartDocument() throws Exception {
        // Файл "dat.xml" (данные) внутри Zip-архива
        newFileOpen("dat.xml");

        // Писатель для XML-файла
        writerXml = new JdxReplicaWriterXml(zipOutputStream);

        // Пишем заголовок
        writerXml.startDocument();
        writerXml.writeReplicaHeader(replica);

        //
        return writerXml;
    }


    /*
     * Утилиты
     */

    /**
     * Возвращает временный файл для реплики
     */
    public static File createTempFileReplica(IReplica replica) throws IOException {
        String fileNameTemplate = UtString.padLeft(String.valueOf(replica.getInfo().getWsId()), 3, '0') + "-" + UtString.padLeft(String.valueOf(replica.getInfo().getAge()), 9, '0');
        File tempFile = File.createTempFile("~jdx-" + fileNameTemplate + "-", ".zip");
        return tempFile;
    }


}
