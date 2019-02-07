package jdtx.repl.main.api;

import javax.xml.stream.*;
import java.io.*;
import java.util.*;

/**
 */
public class ReplicaReaderXml {

    IReplica replica = null;
    XMLStreamReader reader = null;
    private long dbId;

    public ReplicaReaderXml(IReplica replica) throws Exception {
        this.replica = replica;
        //
        InputStream ist = new FileInputStream(replica.getFile());
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        reader = xmlInputFactory.createXMLStreamReader(ist, "utf-8");

        // DbId
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("replica") == 0) {
                    dbId = Long.valueOf(reader.getAttributeValue(null, "DB_ID"));
                    break;
                }
            }
        }
    }

    public long getDbId() {
        return dbId;
    }

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

    public Map nextRec() throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("rec") == 0) {
                    // Значения полей
                    Map values = new HashMap<>();
                    for (int i = 0; i < reader.getAttributeCount(); i++) {
                        String fieldName = reader.getAttributeLocalName(i);
                        values.put(fieldName, reader.getAttributeValue(i));
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


    public void close() throws XMLStreamException {
        reader.close();
    }

}
