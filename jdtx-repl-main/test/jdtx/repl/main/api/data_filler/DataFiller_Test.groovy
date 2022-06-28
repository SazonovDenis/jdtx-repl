package jdtx.repl.main.api.data_filler

import jdtx.repl.main.api.*
import org.junit.*

class DataFiller_Test extends ReplDatabaseStruct_Test {

    IDataFiller filler
    Random rnd

    public Map<String, String> xxx = [
            "Lic"   : [
                    "ins": [
                            count: 10,
                    ],
                    "upd": [
                            count : 3,
                            values: [
                                    "name" : "**-test-***",
                                    "state": [2, 3, 7]
                            ]
                    ],
                    "upd": [
                            ids: [383, 1496, 2042]
                    ]
            ],
            "ULZ"   : "000",
            "REGION": "000",
    ]

    @Test
    public void test() {
        Map<DataFillerPK, DataFillerRec> ulz = filler.ins("Ulz", 20)
        Map<DataFillerPK, DataFillerRec> lic = filler.ins("Lic", 10, ["ulz": ulz.keySet(), "name": "Lic-*****"])
        Map<DataFillerPK, DataFillerRec> pawnChit = filler.ins("PawnChit", 10, ["lic": lic.keySet()])
        for (DataFillerPK pawnChitId : pawnChit.keySet()) {
            filler.ins("PawnChitSubject", 1 + rnd.nextInt(3), ["pawnChit": pawnChitId])
        }
    }

}
