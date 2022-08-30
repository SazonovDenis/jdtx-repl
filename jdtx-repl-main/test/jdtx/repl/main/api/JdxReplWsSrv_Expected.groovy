package jdtx.repl.main.api

import jandcode.bgtasks.*
import jandcode.dbm.data.*
import jandcode.dbm.db.*
import jandcode.utils.*
import jandcode.utils.variant.*
import jdtx.repl.main.api.mailer.*
import jdtx.repl.main.api.manager.*
import jdtx.repl.main.api.struct.*
import jdtx.repl.main.api.util.*
import jdtx.repl.main.task.*
import jdtx.repl.main.ut.*
import org.apache.commons.io.*
import org.junit.*
import org.junit.rules.*

import java.util.concurrent.*


public class JdxReplWsSrv_Expected  {

    public static Map<String, String> equalExpected

    public static Map<String, String> expectedEqual_full = [
            "USRLOG"         : "NYN",
            "PAWNCHIT"       : "NNN",
            "PAWNCHITSUBJECT": "NNN",
            "COMMENTTEXT"    : "NNN",
            "COMMENTTIP"     : "NNN",
            "LIC"            : "NNN",
            "LICDOCVID"      : "NNN",
            "LICDOCTIP"      : "NNN",
            "REGIONTIP"      : "NNN",
            "ULZ"            : "NNN",
            "REGION"         : "NNN",
    ]

    public static Map<String, String> expectedEqual_noFilter = [
            "USRLOG"         : "N?N",
            "PAWNCHIT"       : "NNN",
            "PAWNCHITSUBJECT": "NNN",
            "COMMENTTEXT"    : "NNN",
            "COMMENTTIP"     : "NNN",
            "LIC"            : "NNN",
            "LICDOCVID"      : "NNN",
            "LICDOCTIP"      : "NNN",
            "REGIONTIP"      : "NNN",
            "ULZ"            : "NNN",
            "REGION"         : "NNN",
    ]

    public static  Map<String, String> expectedEqual_filterLic = [
            "USRLOG"         : "NYN",
            "PAWNCHIT"       : "N?N",
            "PAWNCHITSUBJECT": "N?N",
            "COMMENTTEXT"    : "N?N",
            "COMMENTTIP"     : "NNN",
            "LIC"            : "NNN",
            "LICDOCVID"      : "NNN",
            "LICDOCTIP"      : "NNN",
            "REGIONTIP"      : "NNN",
            "ULZ"            : "NNN",
            "REGION"         : "NNN",
    ]

    public  static Map<String, String> expectedNotEqual_2isEmpty = [
            "USRLOG"         : "NYN",
            "PAWNCHIT"       : "NYN",
            "PAWNCHITSUBJECT": "NYN",
            "COMMENTTEXT"    : "NYN",
            "COMMENTTIP"     : "NYN",
            "LIC"            : "NYN",
            "LICDOCVID"      : "NYN",
            "LICDOCTIP"      : "NYN",
            "REGIONTIP"      : "NYN",
            "ULZ"            : "NYN",
            "REGION"         : "NYN",
    ]

    public  static Map<String, String> expectedNotEqual = [
            "USRLOG"         : "YY?",
            "PAWNCHIT"       : "NNN",
            "PAWNCHITSUBJECT": "NNN",
            "COMMENTTEXT"    : "N?Y",
            "COMMENTTIP"     : "NNN",
            "LIC"            : "YYY",
            "LICDOCVID"      : "NNN",
            "LICDOCTIP"      : "NNN",
            "REGIONTIP"      : "NNN",
            "ULZ"            : "???",
            "REGION"         : "N??",
    ]

}

