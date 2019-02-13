package jdtx.repl.main.api;

import groovy.json.*;
import jandcode.utils.test.*;
import org.joda.time.*;
import org.junit.*;

import javax.xml.stream.*;
import java.io.*;

/**
 */
public class XmlRW_Test extends UtilsTestCase {


    @Test
    public void test_escape() throws Exception {
        String s0 = "qqq\tttt\baaa\0zzz\\too\\boo\\roo\\zoo";
        String s1 = StringEscapeUtils.escapeJava(s0);
        String s2 = StringEscapeUtils.unescapeJava(s1);
        System.out.println(s0);
        System.out.println(s1);
        System.out.println(s2);
        assertEquals(s0, s2);
    }

    @Test
    public void test_escape_ru() throws Exception {
        String s0 = "qwerty|Иванов ВАСЯ|ә, ғ, қ, ң, ө, ұ, ү, h";
        String s1 = StringEscapeUtils.escapeJava(s0);
        String s2 = StringEscapeUtils.unescapeJava(s1);
        System.out.println(s0);
        System.out.println(s1);
        System.out.println(s2);
        assertEquals(s0, s2);
    }

    @Test
    public void test_write() throws Exception {
        OutputStream ost = new FileOutputStream("../_test-data/csv.xml");

        XMLOutputFactory xof = XMLOutputFactory.newInstance();
        XMLStreamWriter wr = xof.createXMLStreamWriter(ost, "utf-8");
        wr.writeStartDocument();

        wr.writeStartElement("root");

        wr.writeStartElement("rec");
        wr.writeAttribute("id", String.valueOf(12435061));
        wr.writeAttribute("dt", String.valueOf(new DateTime()));
        wr.writeAttribute("name", "name 1340956");
        wr.writeEndElement();

        wr.writeStartElement("rec");
        wr.writeAttribute("id", String.valueOf(3456.67));
        wr.writeAttribute("dt", String.valueOf(new DateTime()));
        wr.writeAttribute("name", "Строка ә, ғ, қ, ң, ө, ұ, ү, h, 99999");
        wr.writeEndElement();

        wr.writeEndElement();

        wr.writeEndDocument();
        //wr.flush();
        wr.close();
    }

    @Test
    public void test_read() throws Exception {
        InputStream ist = new FileInputStream("../_test-data/csv.xml");

        XMLInputFactory fff = XMLInputFactory.newInstance();
        XMLStreamReader rd = fff.createXMLStreamReader(ist, "utf-8");

        while (rd.hasNext()) {
            int event = rd.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if (rd.getLocalName().compareToIgnoreCase("rec") == 0) {
                    System.out.println(rd.getLocalName());
                    System.out.println(rd.getAttributeCount());
                    for (int n = 0; n < rd.getAttributeCount(); n++) {
                        System.out.println("  " + rd.getAttributeValue(n));
                    }
                }
            }
        }

        rd.close();
    }


}
