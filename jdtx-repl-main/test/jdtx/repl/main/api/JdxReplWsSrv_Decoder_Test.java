package jdtx.repl.main.api;

import org.junit.*;

public class JdxReplWsSrv_Decoder_Test extends JdxReplWsSrv_Test{


    @Test
    public void test_all_filter() throws Exception {
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        cfg_json_decode = "../install/cfg/decode_strategy_194.json";
        prepareEtalon_TestAll();
    }

}
