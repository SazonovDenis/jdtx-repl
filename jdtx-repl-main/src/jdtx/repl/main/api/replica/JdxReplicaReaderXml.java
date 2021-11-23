package jdtx.repl.main.api.replica;

import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import javax.xml.stream.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;

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

    public Map<String, String> nextRec() throws XMLStreamException {
        return UtXmlRW.readRec(reader);
    }

    public void close() throws XMLStreamException, IOException {
        inputStream.close();
    }

    public static void readReplicaInfo(IReplica replica) throws Exception {
        ReplicaInfo info = new ReplicaInfo();
        try (
                InputStream zipInputStream = createInputStream(replica, ".info")
        ) {
            JSONObject jsonObject;
            Reader reader = new InputStreamReader(zipInputStream);
            JSONParser parser = new JSONParser();
            jsonObject = (JSONObject) parser.parse(reader);
            info.fromJSONObject(jsonObject);
        }

        // Тут CRC реплики НЕ забираем!!! Его в .info не может быть
        replica.getInfo().setReplicaType(info.getReplicaType());
        replica.getInfo().setDbStructCrc(info.getDbStructCrc());
        replica.getInfo().setWsId(info.getWsId());
        replica.getInfo().setAge(info.getAge());
        replica.getInfo().setDtFrom(info.getDtFrom());
        replica.getInfo().setDtTo(info.getDtTo());
    }

    public static InputStream createInputStreamData(IReplica replica) throws IOException {
        return createInputStream(replica, ".xml");
    }

    public static InputStream createInputStream(IReplica replica, String dataFileMask) throws IOException {
        InputStream inputStream = null;
        ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(replica.getData()));
        ZipEntry entry;
        while ((entry = zipInputStream.getNextEntry()) != null) {
            String name = entry.getName();
            if (name.endsWith(dataFileMask)) {
                inputStream = zipInputStream;
                break;
            }
        }
        if (inputStream == null) {
            throw new XError("Not found [" + dataFileMask + "] in replica: " + replica.getData());
        }

        return inputStream;
    }


    private IReplicaInfo readReplicaHeader() throws XMLStreamException {
        IReplicaInfo replicaInfo = new ReplicaInfo();

        // Чтение заголовка - WS_ID, AGE и REPLICA_TYPE
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("replica") == 0) {
                    replicaInfo.setReplicaType(Integer.parseInt(reader.getAttributeValue(null, "REPLICA_TYPE")));
                    replicaInfo.setWsId(Long.parseLong(reader.getAttributeValue(null, "WS_ID")));
                    replicaInfo.setAge(Long.parseLong(reader.getAttributeValue(null, "AGE")));
                    break;
                }
            }
        }

        //
        return replicaInfo;
    }


}
