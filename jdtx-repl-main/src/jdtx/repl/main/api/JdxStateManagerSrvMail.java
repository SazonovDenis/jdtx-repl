package jdtx.repl.main.api;

import jandcode.dbm.db.*;

public class JdxStateManagerSrvMail implements IJdxStateManagerMail {

    private Db db;
    private long wsId;
    private String queName;
    private JdxStateManagerSrv stateManagerSrv;

    public JdxStateManagerSrvMail(Db db, long wsId, String queName) {
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
