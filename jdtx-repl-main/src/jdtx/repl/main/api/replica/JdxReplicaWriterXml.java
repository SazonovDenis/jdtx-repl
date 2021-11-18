package jdtx.repl.main.api.replica;

import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.util.*;

import javax.xml.stream.*;
import java.io.*;

/**
 *
 */
public class JdxReplicaWriterXml {

    XMLStreamWriter writer;

    // Статусы писателя
    boolean currentElement_root = false;
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
        currentElement_root = true;
    }

    public void closeDocument() throws Exception {
        // Закрываем каждый уровень
        // <replica>
        if (currentElement_replica) {
            writer.writeEndElement();
            //
            currentElement_replica = false;
        }
        // <rec>
        if (currentElement_rec) {
            writer.writeEndElement();
            //
            currentElement_rec = false;
        }
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

    public void writeOprType(int oprType) throws XMLStreamException {
        // <rec>
        if (!currentElement_rec) {
            throw new XMLStreamException("Not started currentElement_rec");
        }
        //
        writer.writeAttribute(UtJdx.XML_FIELD_OPR_TYPE, String.valueOf(oprType));
    }

    public void writeRecValue(String name, Object value) throws XMLStreamException {
        // <rec>
        if (!currentElement_rec) {
            throw new XMLStreamException("Not started currentElement_rec");
        }

        //
        writer.writeAttribute(name, UtXml.valueToStr(value));
    }

    void writeReplicaInfo(IReplicaInfo replicaInfo) throws XMLStreamException {
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
        writer.writeAttribute("WS_ID", String.valueOf(replicaInfo.getWsId()));
        writer.writeAttribute("AGE", String.valueOf(replicaInfo.getAge()));
        writer.writeAttribute("DT_FROM", String.valueOf(replicaInfo.getDtFrom()));
        writer.writeAttribute("DT_TO", String.valueOf(replicaInfo.getDtTo()));
        writer.writeAttribute("REPLICA_TYPE", String.valueOf(replicaInfo.getReplicaType()));
    }


}
