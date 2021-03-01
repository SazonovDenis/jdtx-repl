package jdtx.repl.main.service;

import java.util.*;

public class ServiceInfoComparator implements Comparator<ServiceInfo> {

    @Override
    public int compare(ServiceInfo serviceInfo1, ServiceInfo serviceInfo2) {
        return serviceInfo1.getServicePath().compareToIgnoreCase(serviceInfo2.getServicePath());
    }

}
