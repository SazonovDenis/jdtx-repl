package jdtx.repl.main.api.data_binder;

import jandcode.dbm.test.*;
import jdtx.repl.main.api.data_binder.*;
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
        Map<String, Object> values = res.getValues();

        while (!res.eof()) {
            System.out.println("id: " + values.get("id") + ", name: " + values.get("name"));
            res.next();
        }

    }

}
