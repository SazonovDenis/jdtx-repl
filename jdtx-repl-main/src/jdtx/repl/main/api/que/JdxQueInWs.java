package jdtx.repl.main.api.que;

import jandcode.dbm.db.*;

public class JdxQueInWs extends JdxQue implements IJdxQue {

    public JdxQueInWs(Db db, String queName, boolean stateMode) {
        super(db, queName, stateMode);
    }

}
