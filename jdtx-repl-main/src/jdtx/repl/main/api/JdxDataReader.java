package jdtx.repl.main.api;

import javax.xml.stream.*;
import java.io.*;

/**
 *
 */
public class JdxDataReader {

    XMLStreamReader rd;

    public JdxDataReader(InputStream ist) throws XMLStreamException {
        XMLInputFactory xif = XMLInputFactory.newInstance();
        XMLStreamReader rd = xif.createXMLStreamReader(ist, "utf-8");
    }

    public void next() throws XMLStreamException {
        rd.next();
    }

    public boolean hasNext() throws Exception {
        return false;
    }

    public void close() throws Exception {
        rd.close();
    }

    /**
     * @return вид операции
     */
    public int getOprType() throws XMLStreamException {
        return 0; //rd.getAttributeValue(0);
    }

    public Object getRecValue(String name) throws XMLStreamException {
        return null;
    }

}
