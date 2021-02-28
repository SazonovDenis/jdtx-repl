package jdtx.repl.main.api.manager;

import jandcode.utils.*;
import jandcode.web.*;
import jdtx.repl.main.api.*;
import org.json.simple.*;
import org.junit.*;

/**
 */
public class CfgManager_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_dbStruct_SaveLoad() throws Exception {
        CfgManager cfgManager = new CfgManager(db);

        //
        String sP_1 = UtFile.loadString("test/jdtx/repl/main/api/UtCfg_Marker_Test.publications.json");
        String sD_1 = UtFile.loadString("test/jdtx/repl/main/api/UtCfg_Marker_Test.decode_strategy.json");
        String sW_1 = UtFile.loadString("test/jdtx/repl/main/api/UtCfg_Marker_Test.srv.json");


        // Пишем
        JSONObject cfg1 = (JSONObject) UtJson.toObject(sW_1);
        cfgManager.setSelfCfg(cfg1, CfgType.WS);

        cfg1 = (JSONObject) UtJson.toObject(sD_1);
        cfgManager.setSelfCfg(cfg1, CfgType.DECODE);

        cfg1 = (JSONObject) UtJson.toObject(sP_1);
        cfgManager.setSelfCfg(cfg1, CfgType.PUBLICATIONS);


        // Читаем
        JSONObject cfg2 = cfgManager.getSelfCfg(CfgType.WS);
        String sW_2 = UtJson.toString(cfg2);

        cfg2 = cfgManager.getSelfCfg(CfgType.DECODE);
        String sD_2 = UtJson.toString(cfg2);

        cfg2 = cfgManager.getSelfCfg(CfgType.PUBLICATIONS);
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

    @Test
    public void test_srvSetCfg() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        //
        long destinationWsId = 2;
        String cfgFileName = "test/jdtx/repl/main/api/UtCfg_Marker_Test.publications.json";
        srv.srvSendCfg(cfgFileName, "cfg_publications", destinationWsId);
    }


}
