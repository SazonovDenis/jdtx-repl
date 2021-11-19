package jdtx.repl.main.api.rec_merge;

import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;

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

    public Map<String, String> nextRec() throws XMLStreamException {
        return UtXmlRW.readRec(reader);
    }

    public void close() throws Exception {
        inputStream.close();
    }

}
