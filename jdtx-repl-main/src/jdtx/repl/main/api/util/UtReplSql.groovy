package jdtx.repl.main.api.util;

/**
 */
public class UtReplSql {

    public static String sql_srv = """
select
  ${UtJdx.SYS_TABLE_PREFIX}srv_workstation_list.*
from
  ${UtJdx.SYS_TABLE_PREFIX}srv_workstation_list
order by
  ${UtJdx.SYS_TABLE_PREFIX}srv_workstation_list.id
"""

}
