package jdtx.repl.main.api.repair;

import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.mailer.*;
import org.joda.time.*;
import org.json.simple.*;

import java.util.*;

public class JdxRepairInfoManager {

    private IMailer mailer;

    public JdxRepairInfoManager(IMailer mailer) {
        this.mailer = mailer;
    }

    /**
     * Запросить разрешение на ремонт
     *
     * @param repairGuid Guid ремонта
     */
    public void setRequestRepair(String repairGuid) throws Exception {
        Map repairInfo = new HashMap();
        repairInfo.put("guid", repairGuid);
        repairInfo.put("dt", new DateTime());
        mailer.setData(repairInfo, "repair.info", null);
    }

    /**
     * Удалить все запросы на ремонт
     */
    public void setNoRepair() throws Exception {
        Map repairInfo = new HashMap();
        mailer.setData(repairInfo, "repair.info", null);
    }

    /**
     * Разрешить ремонт
     *
     * @param repairGuid Guid разрешенного ремонта
     */
    public void setRepairAllowed(String repairGuid) throws Exception {
        Map repairInfo = new HashMap();
        repairInfo.put("allowed", true);
        repairInfo.put("guid", repairGuid);
        repairInfo.put("dt", new DateTime());
        mailer.setData(repairInfo, "repair.info", null);
    }

    /**
     * Узнать какой ремонт разрешен
     *
     * @return Guid разрешенного ремонта
     */
    public String getAllowedRepairGuid() throws Exception {
        JSONObject repairInfo = mailer.getData("repair.info", null);
        JSONObject repairData = (JSONObject) repairInfo.get("data");
        boolean doRepair = UtJdxData.booleanValueOf(repairData.get("allowed"), false);
        String srvRepairGuid = UtJdxData.stringValueOf(repairData.get("guid"), null);
        if (doRepair) {
            return srvRepairGuid;
        } else {
            return null;
        }
    }

    /**
     * @return Guid запрошенного ремонта
     */
    public String getRepairGuid() throws Exception {
        JSONObject repairInfo = mailer.getData("repair.info", null);
        JSONObject repairData = (JSONObject) repairInfo.get("data");
        String srvRepairGuid = UtJdxData.stringValueOf(repairData.get("guid"), null);
        return srvRepairGuid;
    }

}
