package jdtx.repl.main.api.manager;

import jandcode.dbm.db.*;
import org.apache.commons.logging.*;

/**
 * Сервер: отметка состояния рабочих станций: MUTE и UMNMUTE.
 */
public class JdxMuteManagerSrv {

    private Db db;

    //
    protected SrvWorkstationStateManager stateManager;

    //
    protected static Log log = LogFactory.getLog("jdtx.MuteManagerSrv");

    public JdxMuteManagerSrv(Db db) {
        this.db = db;
        stateManager = new SrvWorkstationStateManager(db);
    }

    /**
     * Отметка состояния станции: MUTE
     * Также отмечается номер той реплики в общей очереди queCommon, которой станция отправила подтверждение своего состояния MUTE.
     * Мы можем расчитывать, что пока станция не выйдет из состояния MUTE,
     * новых больше номера commonQueNo реплик в общей очереди queCommon от этой станции не появится.
     */
    public void setMuteDone(long wsId, long queCommonNo) throws Exception {
        log.info("workstation mute done, wsId: " + wsId + ", queCommonNo: " + queCommonNo);
        //
        stateManager.setValue(wsId, "mute_age", queCommonNo);
    }

    /**
     * Отметка состояния станции: UMNMUTE.
     */
    public void setUnmuteDone(long wsId) throws Exception {
        log.info("workstation unmute, wsId: " + wsId);
        //
        stateManager.setValue(wsId, "mute_age", 0);
    }


}
