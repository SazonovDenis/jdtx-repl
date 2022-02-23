package jdtx.repl.main.api.que;

import jandcode.dbm.db.*;
import org.apache.commons.logging.*;

/**
 *
 */
public class JdxQueOut001 extends JdxQueOutSrv {

    public JdxQueOut001(Db db, long destinationWsId) {
        super(db, UtQue.SRV_QUE_OUT001, UtQue.STATE_AT_SRV);
        this.destinationWsId = destinationWsId;
        log = LogFactory.getLog("jdtx.JdxQueOut001");
    }


}
