package jdtx.repl.main.api.que;

import java.util.*;

public interface IJdxQueCommon extends IJdxReplicaStorage, IJdxReplicaQue {

    void setSrvQueIn(Map<Long, IJdxQue> srvQueInList);

}
