package jdtx.repl.main.api.filter;

import java.util.*;

public class RecordFilterTrue implements IRecordFilter {

    @Override
    public boolean isMach(Map<String, Object> recValues) {
        return true;
    }

}
