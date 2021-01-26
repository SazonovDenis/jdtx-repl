package jdtx.repl.main.api.replica;

import jandcode.utils.*;
import jdtx.repl.main.api.*;
import org.joda.time.*;

import javax.xml.stream.*;
import java.io.*;

/**
 *
 */
public class JdxReplicaWriterXml {

    XMLStreamWriter writer;

    // Статусы писателя
    boolean currentElement_replica = false;
    boolean currentElement_table = false;
    boolean currentElement_rec = false;

    //
    public JdxReplicaWriterXml(OutputStream ost) throws XMLStreamException {
        XMLOutputFactory xof = XMLOutputFactory.newInstance();
        writer = xof.createXMLStreamWriter(ost, "utf-8");
    }

    public void startDocument() throws XMLStreamException {
        writer.writeStartDocument();
        writer.writeStartElement("root");
    }

    public void closeDocument() throws Exception {
        // <replica>
        if (currentElement_replica) {
            writer.writeEndElement();
        }
        // <rec>
        if (currentElement_rec) {
            writer.writeEndElement();
        }
        // <table>
        if (currentElement_table) {
            writer.writeEndElement();
        }
        // <root>
        writer.writeEndElement();
        //
        writer.writeEndDocument();
    }

    public void appendRec() throws XMLStreamException {
        // <table>
        if (!currentElement_table) {
            throw new XMLStreamException("Not started currentElement_table");
        }
        // <rec>
        if (currentElement_rec) {
            writer.writeEndElement();
            currentElement_rec = false;
        }
        //
        writer.writeStartElement("rec");
        //
        currentElement_rec = true;
    }

    public void startTable(String tableName) throws XMLStreamException {
        // <currentElement_replica>
        if (currentElement_replica) {
            writer.writeEndElement();
            currentElement_replica = false;
        }
        // <rec>
        if (currentElement_rec) {
            writer.writeEndElement();
            currentElement_rec = false;
        }
        // <table>
        if (currentElement_table) {
            writer.writeEndElement();
            currentElement_table = false;
        }
        //
        writer.writeStartElement("table");
        writer.writeAttribute("name", tableName);
        //
        currentElement_table = true;
    }

    public void flush() throws Exception {
        writer.flush();
    }

    public void close() throws Exception {
        writer.close();
    }

    public void setOprType(int oprType) throws XMLStreamException {
        // <rec>
        if (!currentElement_rec) {
            throw new XMLStreamException("Not started currentElement_rec");
        }
        //
        writer.writeAttribute("Z_OPR", String.valueOf(oprType));
    }

    public void setRecValue(String name, Object value) throws XMLStreamException {
        // <rec>
        if (!currentElement_rec) {
            throw new XMLStreamException("Not started currentElement_rec");
        }

        if (value instanceof byte[]) {
            // Особая сериализация для BLOB
            byte[] blob = (byte[]) value;
            String blobBase64 = UtString.encodeBase64(blob);
            writer.writeAttribute(name, blobBase64);
        } else if (value instanceof DateTime) {
            // Сериализация с или без timezone
            // todo: Проверить сериализацию и десериализацию с/без timezone
            writer.writeAttribute(name, UtDate.toString((DateTime) value));
        } else {
            // Обычная сериализация
            writer.writeAttribute(name, UtStringEscape.escapeJava(String.valueOf(value)));
        }
    }

    public void writeReplicaHeader(IReplica replica) throws XMLStreamException {
        // <table>
        if (currentElement_table) {
            throw new XMLStreamException("Already started currentElement_table");
        }
        // <rec>
        if (currentElement_rec) {
            throw new XMLStreamException("Already started currentElement_rec");
        }
        // <currentElement_replica>
        if (!currentElement_replica) {
            writer.writeStartElement("replica");
            currentElement_replica = true;
        }
        //
        writer.writeAttribute("WS_ID", String.valueOf(replica.getInfo().getWsId()));
        writer.writeAttribute("AGE", String.valueOf(replica.getInfo().getAge()));
        writer.writeAttribute("DT_FROM", String.valueOf(replica.getInfo().getDtFrom()));
        writer.writeAttribute("DT_TO", String.valueOf(replica.getInfo().getDtTo()));
        writer.writeAttribute("REPLICA_TYPE", String.valueOf(replica.getInfo().getReplicaType()));
    }


}
