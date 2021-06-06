package jdtx.repl.main.api.manager;

import jandcode.dbm.db.*;

public class JdxMailStateManagerSrv implements IJdxMailStateManager {

    private Db db;
    private long wsId;
    private String queName;
    private JdxStateManagerSrv stateManagerSrv;

    public JdxMailStateManagerSrv(Db db, long wsId, String queName) {
        this.db = db;
        this.wsId = wsId;
        this.queName = queName;
        this.stateManagerSrv = new JdxStateManagerSrv(db);
    }

    @Override
    public long getMailSendDone() throws Exception {
        return stateManagerSrv.getMailSendDone(wsId, queName);
    }

    @Override
    public void setMailSendDone(long sendDone) throws Exception {
        stateManagerSrv.setMailSendDone(wsId, queName, sendDone);
    }
}
