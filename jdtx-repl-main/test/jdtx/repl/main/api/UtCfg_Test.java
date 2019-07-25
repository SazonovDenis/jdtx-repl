package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.web.*;
import org.json.simple.*;
import org.junit.*;

/**
 */
public class UtCfg_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_Iterate() throws Exception {
        String s = UtFile.loadString("test/etalon/srv.json");
        JSONObject cfg = (JSONObject) UtJson.toObject(s);
        for (Object k : cfg.keySet()) {
            Object o = cfg.get(k);
            System.out.println(k.toString() + ": " + o.toString());
        }
    }

    @Test
    public void test_dbStruct_SaveLoad() throws Exception {
        UtCfg utCfg = new UtCfg(db);

        //
        String sP_1 = UtFile.loadString("test/jdtx/repl/main/api/UtCfg_Test.publication.json");
        String sD_1 = UtFile.loadString("test/jdtx/repl/main/api/UtCfg_Test.decode_strategy.json");
        String sW_1 = UtFile.loadString("test/jdtx/repl/main/api/UtCfg_Test.srv.json");


        // Пишем
        JSONObject cfg1 = (JSONObject) UtJson.toObject(sW_1);
        utCfg.setCfgWs(cfg1);

        cfg1 = (JSONObject) UtJson.toObject(sD_1);
        utCfg.setCfgDecode(cfg1);

        cfg1 = (JSONObject) UtJson.toObject(sP_1);
        utCfg.setCfgPublications(cfg1);


        // Читаем
        JSONObject cfg2 = utCfg.getCfgWs();
        String sW_2 = UtJson.toString(cfg2);

        cfg2 = utCfg.getCfgDecode();
        String sD_2 = UtJson.toString(cfg2);

        cfg2 = utCfg.getCfgPublications();
        String sP_2 = UtJson.toString(cfg2);


        // Форматирование исходных строк для корректного сравнения
        sW_1 = UtJson.toString(UtJson.toObject(sW_1));
        sP_1 = UtJson.toString(UtJson.toObject(sP_1));
        sD_1 = UtJson.toString(UtJson.toObject(sD_1));


        // Сравниваем
        assertEquals(sW_1, sW_2);
        assertEquals(sD_1, sD_2);
        assertEquals(sP_1, sP_2);

        assertNotSame(sW_1, sD_2);
        assertNotSame(sD_1, sP_2);
        assertNotSame(sP_1, sW_2);
    }


}
