package jdtx.repl.main.api;

import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.mailer.*;
import org.json.simple.*;

import java.util.*;

public class JdxDatabaseRepairInfoManager {

    private final IMailer mailer;

    public JdxDatabaseRepairInfoManager(IMailer mailer) {
        this.mailer = mailer;
    }

    void setRepairAllowed(String repairGuid) throws Exception {
        Map repairInfo = new HashMap();
        repairInfo.put("repair", true);
        repairInfo.put("guid", repairGuid);
        mailer.setData(repairInfo, "repair.info", null);
    }

    void setNoRepair() throws Exception {
        Map repairInfo = new HashMap();
        mailer.setData(repairInfo, "repair.info", null);
    }

    public String getAllowedRepairGuid() throws Exception {
        JSONObject repairInfo = mailer.getData("repair.info", null);
        JSONObject repairData = (JSONObject) repairInfo.get("data");
        boolean doRepair = UtJdxData.booleanValueOf(repairData.get("repair"), false);
        String srvRepairGuid = UtJdxData.stringValueOf(repairData.get("guid"), null);
        if (doRepair) {
            return srvRepairGuid;
        } else {
            return null;
        }
    }

    public String getRepairGuid() throws Exception {
        JSONObject repairInfo = mailer.getData("repair.info", null);
        JSONObject repairData = (JSONObject) repairInfo.get("data");
        String srvRepairGuid = UtJdxData.stringValueOf(repairData.get("guid"), null);
        return srvRepairGuid;
    }

    void requestRepair(String repairGuid) throws Exception {
        Map repairInfo = new HashMap();
        repairInfo.put("guid", repairGuid);
        mailer.setData(repairInfo, "repair.info", null);
    }


}
