package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import org.junit.*;

import java.util.*;

/**
 */
public class JdxDataBinder_SelectorById_Test extends DbmTestCase {

    /**
     */
    @Test
    public void testIterator() throws Exception {

        List<Long> idList = new ArrayList<>();
        idList.add(1L);
        idList.add(2L);
        idList.add(3L);
        idList.add(4L);

        Iterator<Long> iterator = idList.iterator();

        while (iterator.hasNext()) {
            System.out.println("id: " + iterator.next());
        }

    }
    /**
     */
    @Test
    public void testRecordSelector() throws Exception {

        List<Long> idList = new ArrayList<>();
        idList.add(1L);
        idList.add(2L);
        idList.add(3L);
        idList.add(4L);

        IJdxDataBinder res = new JdxDataBinder_SelectorById(dbm.getDb(), "region", "*", idList);

        while (!res.eof()) {
            System.out.println("id: " + res.getValue("id") + ", name: " + res.getValue("name"));
            res.next();
        }

    }

}
