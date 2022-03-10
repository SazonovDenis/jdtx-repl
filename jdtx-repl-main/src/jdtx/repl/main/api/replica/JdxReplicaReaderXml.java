package jdtx.repl.main.api.replica;

import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.util.*;
import org.joda.time.*;
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

    JdxReplicaFileInputStream inputStream = null;
    XMLStreamReader reader = null;
    IReplicaInfo replicaHeaderInfo = null;

    public JdxReplicaReaderXml(JdxReplicaFileInputStream inputStream) throws Exception {
        this.inputStream = inputStream;

        //
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        reader = xmlInputFactory.createXMLStreamReader(inputStream, "utf-8");

        // Чтение заголовка - WS_ID, AGE и REPLICA_TYPE
        replicaHeaderInfo = readReplicaHeader();
    }

    public JdxReplicaFileInputStream getInputStream() {
        return inputStream;
    }

    public long getWsId() {
        return replicaHeaderInfo.getWsId();
    }

    public long getAge() {
        return replicaHeaderInfo.getAge();
    }

    public DateTime getDtFrom() {
        return replicaHeaderInfo.getDtFrom();
    }

    public DateTime getDtTo() {
        return replicaHeaderInfo.getDtTo();
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
        InputStream stream = createInputStream(replica, ".info");
        try {
            JSONObject jsonObject;
            Reader reader = new InputStreamReader(stream);
            JSONParser parser = new JSONParser();
            jsonObject = (JSONObject) parser.parse(reader);
            info.fromJSONObject(jsonObject);
        } finally {
            stream.close();
        }

        // Тут CRC реплики НЕ забираем!!! Его в .info не может быть
        replica.getInfo().setReplicaType(info.getReplicaType());
        replica.getInfo().setDbStructCrc(info.getDbStructCrc());
        replica.getInfo().setWsId(info.getWsId());
        replica.getInfo().setAge(info.getAge());
        replica.getInfo().setDtFrom(info.getDtFrom());
        replica.getInfo().setDtTo(info.getDtTo());
    }

    public static JdxReplicaFileInputStream createInputStreamData(IReplica replica) throws IOException {
        return createInputStream(replica, ".xml");
    }

    public static JdxReplicaFileInputStream createInputStream(IReplica replica, String dataFileMask) throws IOException {
        JdxReplicaFileInputStream contentInputStream = null;
        FileInputStream inputStream = new FileInputStream(replica.getData());
        JdxReplicaFileInputStream zipInputStream = new JdxReplicaFileInputStream(inputStream);

        try {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                String name = entry.getName();
                if (name.endsWith(dataFileMask)) {
                    contentInputStream = zipInputStream;
                    break;
                }
            }
            if (contentInputStream == null) {
                throw new XError(UtJdxErrors.message_replicaNotFoundContent + ", content: [" + dataFileMask + "], replica: " + replica.getData());
            }
        } catch (Exception e) {
            zipInputStream.close();
            throw e;
        }

        return contentInputStream;
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
                    replicaInfo.setDtFrom(UtJdxData.dateTimeValueOf(reader.getAttributeValue(null, "DT_FROM")));
                    replicaInfo.setDtTo(UtJdxData.dateTimeValueOf(reader.getAttributeValue(null, "DT_TO")));
                    break;
                }
            }
        }

        //
        return replicaInfo;
    }


}
