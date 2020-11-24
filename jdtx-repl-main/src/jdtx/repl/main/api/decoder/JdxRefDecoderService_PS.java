package jdtx.repl.main.api.decoder;

import jandcode.dbm.db.*;

public class JdxRefDecoderService_PS extends JdxRefDecoderService {

    @Override
    public IRefDecoder createRefDecoder(Db db, long self_ws_id) throws Exception {
        return new RefDecoder(db, self_ws_id);
    }

}
