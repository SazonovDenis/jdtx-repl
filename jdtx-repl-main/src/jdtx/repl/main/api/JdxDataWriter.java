package jdtx.repl.main.api;

import javax.xml.stream.*;
import java.io.*;

/**
 *
 */
public class JdxDataWriter {

    XMLStreamWriter wr;
    boolean currentElement_rec = false;
    boolean currentElement_table = false;

    public JdxDataWriter(OutputStream ost) throws XMLStreamException {
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

    public void flush() throws Exception {
        wr.flush();
    }

    public void close() throws Exception {
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

    /**
     * @param oprType вид операции
     */
    public void setOprType(int oprType) throws XMLStreamException {
        wr.writeAttribute("Z_OPR", String.valueOf(oprType));
    }

    public void setRecValue(String name, Object value) throws XMLStreamException {
        wr.writeAttribute(name, String.valueOf(value));
    }

    public void startTable(String tableName) throws XMLStreamException {
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
}
