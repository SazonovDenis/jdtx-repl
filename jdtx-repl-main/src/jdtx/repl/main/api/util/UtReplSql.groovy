package jdtx.repl.main.api.util;

/**
 */
public class UtReplSql {

    public static String sql_srv = """
select
  z_z_srv_workstation_list.id,
  z_z_srv_workstation_list.name,
  z_z_srv_workstation_list.guid,
  z_z_srv_workstation_state.que_in_no_done,                             -- получено от станции
  z_z_srv_workstation_state.que_common_dispatch_done as dispatch_done,  -- выложено в очередь для станции
  z_z_srv_workstation_state.enabled as enabled,
  z_z_srv_workstation_state.mute_age as mute_age
from
  z_z_srv_workstation_list
  join z_z_srv_workstation_state on (z_z_srv_workstation_list.id = z_z_srv_workstation_state.ws_id)
"""

}
