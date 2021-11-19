package jdtx.repl.main.api.data_serializer;

import groovy.json.*;
import jdtx.repl.main.api.util.*;

import javax.xml.stream.*;
import java.util.*;

public class UtXmlRW {

    /**
     * Читает запись
     */
    public static Map<String, String> readRec(XMLStreamReader reader) throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (reader.getLocalName().compareToIgnoreCase("rec") == 0) {
                    // Значения полей
                    Map<String, String> values = readAttributes(reader);
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

    /**
     * Читает значения атрибутов
     */
    public static Map<String, String> readAttributes(XMLStreamReader reader) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String name = reader.getAttributeLocalName(i);
            String value = reader.getAttributeValue(i);
            value = StringEscapeUtils.unescapeJava(value);
            values.put(name, value);
        }
        //
        return values;
    }

    /**
     * Записывает значения атрибута
     */
    public static void writeAttribute(XMLStreamWriter writer, String name, String value) throws XMLStreamException {
        value = UtStringEscape.escapeJava(value);
        writer.writeAttribute(name, value);
    }

}
