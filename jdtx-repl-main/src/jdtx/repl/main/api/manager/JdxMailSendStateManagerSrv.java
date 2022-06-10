package jdtx.repl.main.api.manager;

import jandcode.dbm.db.*;

public class JdxMailSendStateManagerSrv implements IJdxMailSendStateManager {

    private long wsId;
    private String queName;
    private JdxStateManagerSrv stateManagerSrv;

    public JdxMailSendStateManagerSrv(Db db, long wsId, String queName) {
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
