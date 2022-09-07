package jdtx.repl.main.api.manager;

import jandcode.dbm.db.*;

/**
 * Состояние серверных задач: отметки в БД, насколько отработаны очереди и прочее.
 */
public class JdxStateManagerSrv {

    private Db db;

    protected SrvWorkstationStateManager stateManager;

    public JdxStateManagerSrv(Db db) {
        this.db = db;
        stateManager = new SrvWorkstationStateManager(db);
    }


    /**
     * @return Номер реплики, до которого получена
     * очередь out от рабочей станции и помещена в зеркальную очереди queInSrv.
     */
    public long getWsQueInNoReceived(long wsId) throws Exception {
        return stateManager.getValue(wsId, "que_in_no");
    }

    /**
     * Отмечает номер реплики, до которого получена
     * очередь out от рабочей станции и помещена в зеркальную очередь queInSrv.
     */
    public void setWsQueInNoReceived(long wsId, long queInNo) throws Exception {
        stateManager.setValue(wsId, "que_in_no", queInNo);
    }


    /**
     * @return Номер реплики, до которого очередь out от рабочей станции выложена в общую очередь common
     */
    public long getWsQueInNoDone(long wsId) throws Exception {
        return stateManager.getValue(wsId, "que_in_no_done");
    }

    /**
     * Устанавливает возраст реплики, до которого обработана
     * очередь out от рабочей станции при формировании общей очереди.
     */
    public void setWsQueInNoDone(long wsId, long queInNoDone) throws Exception {
        stateManager.setValue(wsId, "que_in_no_done", queInNoDone);
    }


    /**
     * @return Номер реплики, до которого обработана общая очередь
     * при тиражировании реплик для рабочей станции wsId.
     */
    public long getDispatchDoneQueCommon(long wsId) throws Exception {
        return stateManager.getValue(wsId, "que_common_dispatch_done");
    }

    /**
     * Устанавливает номер реплики, до которого обработана общая очередь
     * при тиражировании реплик для рабочей станции wsId.
     */
    public void setDispatchDoneQueCommon(long wsId, long queCommonNoDone) throws Exception {
        stateManager.setValue(wsId, "que_common_dispatch_done", queCommonNoDone);
    }


    /**
     * @return Отметка: до какого номера отправлена очередь queName для рабочей станции wsId
     */
    public long getMailSendDone(long wsId, String queName) throws Exception {
        return stateManager.getValue(wsId, "que_" + queName + "_send_done");
    }

    public void setMailSendDone(long wsId, String queName, long queNoDone) throws Exception {
        stateManager.setValue(wsId, "que_" + queName + "_send_done", queNoDone);
    }


}
