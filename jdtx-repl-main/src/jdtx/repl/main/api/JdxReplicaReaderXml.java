package jdtx.repl.main.api;

import groovy.json.StringEscapeUtils;
import jandcode.utils.error.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.*;

/**
 * ^c Читатель (JdxReplicaReader) создается из файла, а писатель - почему-то из потока
 */
public class JdxReplicaReaderXml {

    InputStream inputStream = null;
    XMLStreamReader reader = null;
    private long wsId;
    private long age;
    private int replicaType;

    public JdxReplicaReaderXml(File file) throws Exception {
        inputStream = new FileInputStream(file);
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        reader = xmlInputFactory.createXMLStreamReader(inputStream, "utf-8");

        // Чтение заголовка - WS_ID, AGE и REPLICA_TYPE
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("replica") == 0) {
                    wsId = Long.valueOf(reader.getAttributeValue(null, "WS_ID"));
                    age = Long.valueOf(reader.getAttributeValue(null, "AGE"));
                    replicaType = Integer.valueOf(reader.getAttributeValue(null, "REPLICA_TYPE"));
                    break;
                }
            }
        }
    }

    public long getWsId() {
        return wsId;
    }

    public long getAge() {
        return age;
    }

    public int getReplicaType() {
        return replicaType;
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
                        values.put(fieldName, StringEscapeUtils.unescapeJava(reader.getAttributeValue(i)));
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

    public void close() throws XMLStreamException, IOException {
        //reader.close();
        inputStream.close();
    }


    public static void readReplicaInfo(IReplica replica) throws Exception {
        JdxReplicaReaderXml reader = new JdxReplicaReaderXml(replica.getFile());
        try {
            replica.setWsId(reader.getWsId());
            replica.setAge(reader.getAge());
            replica.setReplicaType(reader.getReplicaType());
        } finally {
            reader.close();
        }
    }

    public static void readReplicaInfo_zip(IReplica replica) throws Exception {
        JdxReplInfo info = null;
        try (
                ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(replica.getFile()))
        ) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                String name = entry.getName(); // получим название файла

                //
                if (name.endsWith(".info")) {
                    JSONObject jsonObject;
                    Reader reader = new InputStreamReader(zipInputStream);
                    JSONParser parser = new JSONParser();
                    jsonObject = (JSONObject) parser.parse(reader);
                    info = JdxReplInfo.fromJSONObject(jsonObject);
                }

                //
                zipInputStream.closeEntry();
            }
        }

        //
        if (info == null) {
            throw new XError("Replica .info not found");
        }

        //
        replica.setWsId(info.wsId);
        replica.setAge(info.age);
        replica.setReplicaType(info.replicaType);
    }


}
