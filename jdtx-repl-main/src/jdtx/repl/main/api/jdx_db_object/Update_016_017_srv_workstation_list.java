package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

/**
 *
 */
public class Update_016_017_srv_workstation_list implements ISqlScriptExecutor {

    protected Log log = LogFactory.getLog("jdtx.Update_016_017_srv_workstation_list");

    @Override
    public void exec(Db db) throws Exception {
        // Мы сервер?
        DataRecord rec = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO").getCurRec();
        long selfWsId = rec.getValueLong("ws_id");
        if (selfWsId != 1) {
            return;
        }

        // Чтение структуры БД
        IJdxDbStructReader structReader = new JdxDbStructReader();
        structReader.setDb(db);
        IJdxDbStruct structActual = structReader.readDbStruct();

        //
        CfgManager cfgManager = new CfgManager(db);

        // Мы сервер - записываем для всех станций
        DataStore st = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST");
        for (DataRecord recWs : st) {
            long wsId = recWs.getValueLong("id");

            // Чтение конфигурации
            JSONObject cfgPublications = cfgManager.getWsCfg(CfgType.PUBLICATIONS, wsId);

            // Правила публикаций
            IPublicationRuleStorage publicationIn = PublicationRuleStorage.loadRules(cfgPublications, structActual, "in");
            IPublicationRuleStorage publicationOut = PublicationRuleStorage.loadRules(cfgPublications, structActual, "out");

            // Узнаем, какая получится рабочая структура
            // убирание того, чего нет ни в одном из правил публикаций publicationIn и publicationOut
            IJdxDbStruct struct = UtRepl.getStructCommon(structActual, publicationIn, publicationOut);

            //
            cfgManager.setWsStruct(struct, wsId);
        }
    }


}