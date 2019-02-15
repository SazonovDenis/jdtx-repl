package jdtx.repl.main.api;

import groovy.json.*;

import javax.xml.stream.*;
import java.io.*;

/**
 *
 */
public class JdxReplicaWriterXml {

    XMLStreamWriter wr;

    // Статусы писателя
    boolean currentElement_replica = false;
    boolean currentElement_rec = false;
    boolean currentElement_table = false;

    //
    public JdxReplicaWriterXml(OutputStream ost) throws XMLStreamException {
        XMLOutputFactory xof = XMLOutputFactory.newInstance();
        wr = xof.createXMLStreamWriter(ost, "utf-8");
        //
        wr.writeStartDocument();
        wr.writeStartElement("root");
    }

    public void append() throws XMLStreamException {
        // <table>
        if (!currentElement_table) {
            throw new XMLStreamException("Not started currentElement_table");
        }
        // <rec>
        if (currentElement_rec) {
            wr.writeEndElement();
            currentElement_rec = false;
        }
        //
        wr.writeStartElement("rec");
        //
        currentElement_rec = true;
    }

    public void startTable(String tableName) throws XMLStreamException {
        // <currentElement_replica>
        if (currentElement_replica) {
            wr.writeEndElement();
            currentElement_replica = false;
        }
        // <rec>
        if (currentElement_rec) {
            wr.writeEndElement();
            currentElement_rec = false;
        }
        // <table>
        if (currentElement_table) {
            wr.writeEndElement();
            currentElement_table = false;
        }
        //
        wr.writeStartElement("table");
        wr.writeAttribute("name", tableName);
        //
        currentElement_table = true;
    }

    public void flush() throws Exception {
        wr.flush();
    }

    public void close() throws Exception {
        // <replica>
        if (currentElement_replica) {
            wr.writeEndElement();
        }
        // <rec>
        if (currentElement_rec) {
            wr.writeEndElement();
        }
        // <table>
        if (currentElement_table) {
            wr.writeEndElement();
        }
        // <root>
        wr.writeEndElement();
        //
        wr.writeEndDocument();
        //
        wr.close();
    }

    public void setOprType(int oprType) throws XMLStreamException {
        // <rec>
        if (!currentElement_rec) {
            throw new XMLStreamException("Not started currentElement_rec");
        }
        //
        wr.writeAttribute("Z_OPR", String.valueOf(oprType));
    }

    public void setRecValue(String name, Object value) throws XMLStreamException {
        // <rec>
        if (!currentElement_rec) {
            throw new XMLStreamException("Not started currentElement_rec");
        }
        //
        wr.writeAttribute(name, JdxStringEscapeUtils.escapeJava(String.valueOf(value)));
    }

    public void setReplicaInfo(long dbId, long age) throws XMLStreamException {
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
            wr.writeStartElement("replica");
            currentElement_replica = true;
        }
        //
        wr.writeAttribute("DB_ID", String.valueOf(dbId));
        wr.writeAttribute("AGE", String.valueOf(age));
    }

}
