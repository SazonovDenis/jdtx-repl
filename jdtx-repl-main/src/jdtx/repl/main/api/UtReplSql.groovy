package jdtx.repl.main.api;

/**
 */
public class UtReplSql {

    public static String sql_srv = """
select
  z_z_workstation_list.*,
  z_z_state_ws.que_in_age_done as que_in_done,             -- получено от станции
  z_z_state_ws.que_common_dispatch_done as dispatch_done,  -- выложено в очередь для станции
  z_z_state_ws.enabled as enabled,
  z_z_state_ws.mute_age as mute_age
from
  z_z_workstation_list
  join z_z_state_ws on (z_z_workstation_list.id = z_z_state_ws.ws_id)
"""

}
