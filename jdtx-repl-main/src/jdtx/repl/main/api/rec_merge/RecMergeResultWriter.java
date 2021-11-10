package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import jdtx.repl.main.api.util.*;
import org.joda.time.*;

import javax.xml.stream.*;
import java.io.*;
import java.util.zip.*;

public class RecMergeResultWriter {


    XMLStreamWriter writer;
    OutputStream outputStream;
    ZipOutputStream zipOutputStream;

    // Статусы писателя
    boolean currentElement_root = false;
    boolean currentElement_table = false;


    public void open(File file) throws Exception {
        // Формируем файл
        outputStream = new FileOutputStream(file);
        // Zip-архив пишет в файл
        zipOutputStream = new ZipOutputStream(outputStream);
        // Файл "dat.xml" внутри Zip-архива
        ZipEntry zipEntryHead = new ZipEntry("dat.xml");
        zipOutputStream.putNextEntry(zipEntryHead);
        // XML-писатель пишет в Zip-архив
        XMLOutputFactory xof = XMLOutputFactory.newInstance();
        writer = xof.createXMLStreamWriter(zipOutputStream, "utf-8");
        //
        startDocument();
    }

    public void close() throws Exception {
        closeDocument();

        // XML-писатель заканчивает
        writer.flush();
        writer.close();

        // Заканчиваем запись в в zip-архив
        zipOutputStream.closeEntry();
        zipOutputStream.finish();
        zipOutputStream.close();

        // Закрываем файл
        outputStream.close();

    }


    public void writeTableItem(MergeResultTableItem tableItem) throws XMLStreamException {
        // Закрываем уровень

        // <table>
        if (currentElement_table) {
            writer.writeEndElement();
            currentElement_table = false;
        }

        // Открываем уровень
        // <table>
        writer.writeStartElement("table");
        writer.writeAttribute("name", tableItem.tableName);
        writer.writeAttribute("operation", String.valueOf(tableItem.tableOperation));
        //
        currentElement_table = true;
    }

    public void writeRec(DataRecord rec) throws XMLStreamException {
        // <table>
        if (!currentElement_table) {
            throw new XMLStreamException("Not started currentElement_table");
        }

        //
        writer.writeStartElement("rec");

        //
        for (String name : rec.getValues().keySet()) {
            Object value = rec.getValue(name);

            //
            writer.writeAttribute(name, UtXml.valueToStr(value));
        }

        //
        writer.writeEndElement();
    }

    void startDocument() throws XMLStreamException {
        writer.writeStartDocument();
        writer.writeStartElement("root");
        currentElement_root = true;
    }

    void closeDocument() throws Exception {
        // Закрываем каждый уровень

        // <table>
        if (currentElement_table) {
            writer.writeEndElement();
            //
            currentElement_table = false;
        }

        // Закрываем документ
        if (currentElement_root) {
            // <root>
            writer.writeEndElement();
            //
            writer.writeEndDocument();
            //
            currentElement_root = false;
        }
    }


}
