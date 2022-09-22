package jdtx.repl.main.api.util;

class UtReplSql {

    public static String sql_SrvState(String whereCond) {
        if (whereCond == null || whereCond.length() == 0) {
            whereCond = "1=1"
        }
        return "select\n" +
                "  WORKSTATION_LIST.id as WS_ID,\n" +
                "  WORKSTATION_LIST.NAME,\n" +
                "  WORKSTATION_LIST.GUID,\n" +
                "  STATE__ENABLED.param_value as ENABLED,\n" +
                "  STATE__QUE_IN_NO_DONE.param_value as QUE_IN_NO_DONE,\n" +
                "  STATE__MUTE_AGE.param_value as MUTE_AGE,\n" +
                "  '' as MUTE_STATE\n" +
                "from\n" +
                "  " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST WORKSTATION_LIST\n" +
                "  left join " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE STATE__ENABLED" +
                "    on (WORKSTATION_LIST.id = STATE__ENABLED.ws_id and STATE__ENABLED.param_name = 'enabled')\n" +
                "  left join " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE STATE__MUTE_AGE" +
                "    on (WORKSTATION_LIST.id = STATE__MUTE_AGE.ws_id and STATE__MUTE_AGE.param_name = 'mute_age')\n" +
                "  left join " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE STATE__QUE_IN_NO_DONE" +
                "    on (WORKSTATION_LIST.id = STATE__QUE_IN_NO_DONE.ws_id and STATE__QUE_IN_NO_DONE.param_name = 'que_in_no_done')\n" +
                "where\n" +
                "  " + whereCond + "\n" +
                "order by \n" +
                "  WORKSTATION_LIST.id";
    }
}
