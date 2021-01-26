package jdtx.repl.main.api.rec_merge;

import javax.xml.stream.*;
import java.io.*;
import java.util.*;

/**
 * @deprecated Сейчас используется UtRecMergeReader с JSON
 */
@Deprecated
public class UtRecMergeReaderXml {

    InputStream inputStream = null;
    XMLStreamReader reader = null;

    private String tableName = null;

    public UtRecMergeReaderXml(InputStream inputStream) throws Exception {
        this.inputStream = inputStream;

        //
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        reader = xmlInputFactory.createXMLStreamReader(inputStream, "utf-8");
    }

/*
    public String nextTable() throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("table") == 0) {
                    String tableName = reader.getAttributeValue(null, "name");
                    return tableName;
                }
            }
        }

        return null;
    }
*/

    public Map nextRec() throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("table") == 0) {
                    String tableName = reader.getAttributeValue(null, "name");
                    this.tableName = tableName;
                } else if (reader.getLocalName().compareToIgnoreCase("rec") == 0) {
                    // Значения полей
                    Map values = new HashMap<>();
                    for (int i = 0; i < reader.getAttributeCount(); i++) {
                        String fieldName = reader.getAttributeLocalName(i);
                        String fieldValue = reader.getAttributeValue(i);
                        values.put(fieldName, fieldValue);
                    }
                    //
                    values.put("table", this.tableName);
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
                    continue;
                }
            }
        }

        return null;
    }

    public void close() throws XMLStreamException, IOException {
        inputStream.close();
    }


}
