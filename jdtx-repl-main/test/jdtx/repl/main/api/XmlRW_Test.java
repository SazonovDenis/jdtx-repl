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
        String sSr1 = "qqq\tttt\baaa\0zzz\ruuu\nsss\\too\\boo\\roo\\zoo";
        String sSr2 = "qwerty|Иванов ВАСЯ|ә, ғ, қ, ң, ө, ұ, ү, h";
        String sSr3 = "qqq\"\"zoo''hhh\"'xxx'\"fff| the \"SUN\" the 'sun'";

        String sEs1 = StringEscapeUtils.escapeJava(sSr1);
        String sEs2 = StringEscapeUtils.escapeJava(sSr2);
        String sEs3 = StringEscapeUtils.escapeJava(sSr3);
        String sUn1 = StringEscapeUtils.unescapeJava(sEs1);
        String sUn2 = StringEscapeUtils.unescapeJava(sEs2);
        String sUn3 = StringEscapeUtils.unescapeJava(sEs3);
        
        System.out.println(sSr1);
        System.out.println(sEs1);
        System.out.println(sUn1);
        System.out.println("---");
        System.out.println(sSr2);
        System.out.println(sEs2);
        System.out.println(sUn3);
        System.out.println("---");
        System.out.println(sSr3);
        System.out.println(sEs3);
        System.out.println(sUn3);

        assertEquals(sSr1, sUn1);
        assertEquals(sSr2, sUn2);
        assertEquals(sSr3, sUn3);
    }

    @Test
    public void test_escape_Jdx() throws Exception {
        String sSr1 = "qqq\tttt\baaa\0zzz\ruuu\nsss\\too\\boo\\roo\\zoo";
        String sSr2 = "qwerty|Иванов ВАСЯ|ә, ғ, қ, ң, ө, ұ, ү, h";
        String sSr3 = "qqq\"\"zoo''hhh\"'xxx'\"fff| the \"SUN\" the 'sun'";

        String sEs1 = JdxStringEscapeUtils.escapeJava(sSr1);
        String sEs2 = JdxStringEscapeUtils.escapeJava(sSr2);
        String sEs3 = JdxStringEscapeUtils.escapeJava(sSr3);
        String sUn1 = StringEscapeUtils.unescapeJava(sEs1);
        String sUn2 = StringEscapeUtils.unescapeJava(sEs2);
        String sUn3 = StringEscapeUtils.unescapeJava(sEs3);

        System.out.println(sSr1);
        System.out.println(sEs1);
        System.out.println(sUn1);
        System.out.println("---");
        System.out.println(sSr2);
        System.out.println(sEs2);
        System.out.println(sUn3);
        System.out.println("---");
        System.out.println(sSr3);
        System.out.println(sEs3);
        System.out.println(sUn3);

        assertEquals(sSr1, sUn1);
        assertEquals(sSr2, sUn2);
        assertEquals(sSr3, sUn3);
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
