package jdtx.repl.main.api;

import javax.xml.stream.*;
import java.io.*;

/**
 *
 */
public class JdxReplicaWriterXml {

    XMLStreamWriter writer;

    // Статусы писателя
    boolean currentElement_replica = false;
    boolean currentElement_rec = false;
    boolean currentElement_table = false;

    //
    public JdxReplicaWriterXml(OutputStream ost) throws XMLStreamException {
        XMLOutputFactory xof = XMLOutputFactory.newInstance();
        writer = xof.createXMLStreamWriter(ost, "utf-8");
        //
        writer.writeStartDocument();
        writer.writeStartElement("root");
    }

    public void append() throws XMLStreamException {
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
        //
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
        //
        writer.writeAttribute(name, JdxStringEscapeUtils.escapeJava(String.valueOf(value)));
    }

    public void writeReplicaInfo(long wsId, long age) throws XMLStreamException {
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
        writer.writeAttribute("WS_ID", String.valueOf(wsId));
        writer.writeAttribute("AGE", String.valueOf(age));
    }

}
