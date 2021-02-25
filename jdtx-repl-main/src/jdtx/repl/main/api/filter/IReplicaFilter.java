package jdtx.repl.main.api.filter;

import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;

import java.util.*;

public interface IReplicaFilter {

    /**
     * Параметры.
     * Произвольные, состав зависит от потребностей вычисления выражений в фильрах.
     */
    Map<String, String> getFilterParams();

    /**
     * Преобразовываем реплику replicaSrc для рабочей станции по правилам (фильтрам) publicationRules
     */
    IReplica prepareReplicaForWs(IReplica replicaSrc, IPublicationStorage publicationRules) throws Exception;

}
