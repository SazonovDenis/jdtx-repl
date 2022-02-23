package jdtx.repl.main.api.que;

import jandcode.dbm.db.*;
import org.apache.commons.logging.*;

/**
 *
 */
public class JdxQueOut000 extends JdxQueOutSrv {

    public JdxQueOut000(Db db, long destinationWsId) {
        super(db, UtQue.SRV_QUE_OUT000, UtQue.STATE_AT_SRV);
        this.destinationWsId = destinationWsId;
        log = LogFactory.getLog("jdtx.JdxQueOut000");
    }


}
