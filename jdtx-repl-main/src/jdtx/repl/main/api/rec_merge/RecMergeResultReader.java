package jdtx.repl.main.api.rec_merge;

import groovy.json.*;
import jandcode.utils.error.*;

import javax.xml.stream.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;

public class RecMergeResultReader {

    InputStream inputStream;
    XMLStreamReader reader;

    public RecMergeResultReader(InputStream fileInputStream) throws Exception {
        this.inputStream = createInputStream(fileInputStream, "dat.xml");

        //
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        reader = xmlInputFactory.createXMLStreamReader(inputStream, "utf-8");
    }

    public static InputStream createInputStream(InputStream fileInputStream, String dataFileMask) throws IOException {
        InputStream inputStream = null;
        ZipInputStream zipInputStream = new ZipInputStream(fileInputStream);
        ZipEntry entry;
        while ((entry = zipInputStream.getNextEntry()) != null) {
            String name = entry.getName();
            if (name.endsWith(dataFileMask)) {
                inputStream = zipInputStream;
                break;
            }
        }
        if (inputStream == null) {
            throw new XError("Not found [" + dataFileMask + "] in file");
        }

        return inputStream;
    }


    public MergeResultTableItem nextResultTable() throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("table") == 0) {
                    String tableName = reader.getAttributeValue(null, "name");
                    MergeOprType tableResultType = MergeOprType.valueOf(reader.getAttributeValue(null, "operation"));
                    return new MergeResultTableItem(tableName, tableResultType);
                }
            }
        }

        return null;
    }

    public Map<String, Object> nextRec() throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("rec") == 0) {
                    // Значения полей
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < reader.getAttributeCount(); i++) {
                        String fieldName = reader.getAttributeLocalName(i);
                        String fieldValue = reader.getAttributeValue(i);
                        // Грязный хак: BLOB в MIME-кодировке НЕ нуждются в маскировке,
                        // а зато StringEscapeUtils.unescapeJava() падает на больших строках,
                        // выдает "Java heap space".
                        // todo Хорошо бы не по длине судить, а по более надежому критерию!
                        if (fieldValue.length() < 1024 * 10) {
                            values.put(fieldName, StringEscapeUtils.unescapeJava(fieldValue));
                        } else {
                            values.put(fieldName, fieldValue);
                        }
                    }
                    //
                    return values;
                }
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("rec") == 0) {
                    // кончился <rec>
                    continue;
                }
                if (reader.getLocalName().compareToIgnoreCase("table") == 0) {
                    // кончился <table>
                    break;
                }
            }
        }

        return null;
    }

    public void close() throws Exception {
        inputStream.close();
    }

}
