package jdtx.repl.main.api.replica;

import groovy.json.*;
import jdtx.repl.main.api.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import javax.xml.stream.*;
import java.io.*;
import java.util.*;

/**
 * todo: Читатель (JdxReplicaReader) создается из файла, а писатель - почему-то из потока
 */
public class JdxReplicaReaderXml {

    InputStream inputStream = null;
    XMLStreamReader reader = null;
    IReplicaInfo replicaHeaderInfo = null;

    public JdxReplicaReaderXml(InputStream inputStream) throws Exception {
        this.inputStream = inputStream;

        //
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        reader = xmlInputFactory.createXMLStreamReader(inputStream, "utf-8");

        // Чтение заголовка - WS_ID, AGE и REPLICA_TYPE
        replicaHeaderInfo = readReplicaHeader();
    }

    public long getWsId() {
        return replicaHeaderInfo.getWsId();
    }

    public long getAge() {
        return replicaHeaderInfo.getAge();
    }

    public int getReplicaType() {
        return replicaHeaderInfo.getReplicaType();
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

    public void close() throws XMLStreamException, IOException {
        inputStream.close();
    }

    public static void readReplicaInfo(IReplica replica) throws Exception {
        ReplicaInfo info;
        try (
                InputStream zipInputStream = UtRepl.createInputStream(replica, ".info")
        ) {
            JSONObject jsonObject;
            Reader reader = new InputStreamReader(zipInputStream);
            JSONParser parser = new JSONParser();
            jsonObject = (JSONObject) parser.parse(reader);
            info = ReplicaInfo.fromJSONObject(jsonObject);
        }

        //
        replica.getInfo().setReplicaType(info.getReplicaType());
        replica.getInfo().setDbStructCrc(info.getDbStructCrc());
        replica.getInfo().setWsId(info.getWsId());
        replica.getInfo().setAge(info.getAge());
        replica.getInfo().setDtFrom(info.getDtFrom());
        replica.getInfo().setDtTo(info.getDtTo());
    }

    private IReplicaInfo readReplicaHeader() throws XMLStreamException {
        IReplicaInfo replicaInfo = new ReplicaInfo();

        // Чтение заголовка - WS_ID, AGE и REPLICA_TYPE
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("replica") == 0) {
                    replicaInfo.setReplicaType(Integer.valueOf(reader.getAttributeValue(null, "REPLICA_TYPE")));
                    replicaInfo.setWsId(Long.valueOf(reader.getAttributeValue(null, "WS_ID")));
                    replicaInfo.setAge(Long.valueOf(reader.getAttributeValue(null, "AGE")));
                    break;
                }
            }
        }

        //
        return replicaInfo;
    }


}
