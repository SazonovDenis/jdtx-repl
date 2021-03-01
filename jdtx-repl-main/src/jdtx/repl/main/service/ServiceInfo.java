package jdtx.repl.main.service;

import java.util.*;

public class ServiceInfo extends HashMap<String, String> {

    public ServiceInfo(List<String> lst) {
        super();

        for (String s : lst) {
            parse(s, "Имя задачи:", "ServiceName");
            parse(s, "Задача для выполнения:", "ExecutableTask");
            parse(s, "Рабочая папка:", "ExecutableDirectory");
            parse(s, "Время прошлого запуска:", "LastStartTime");
            parse(s, "Время следующего запуска:", "NextStartTime");
            parse(s, "Запуск от имени:", "UserName");
        }

        //
        put("ServiceName", get("ServiceName").substring(1));
    }

    private void parse(String s, String keyOriginal, String key) {
        if (s.contains(keyOriginal)) {
            this.put(key, s.split(keyOriginal)[1].trim());
        }
    }

    public String getServicePath() {
        return get("ExecutableDirectory") + get("ExecutableTask");
    }

    public static void printList(List<ServiceInfo> serviceList) {
        if (serviceList.size() == 0) {
            System.out.println("No services found");
        } else {
            serviceList.sort(new ServiceInfoComparator());
            //
            String executableDirectoryPrior = "";
            for (ServiceInfo serviceInfo : serviceList) {
                String executableDirectory = serviceInfo.get("ExecutableDirectory");
                if (executableDirectory.compareToIgnoreCase(executableDirectoryPrior) != 0) {
                    executableDirectoryPrior = executableDirectory;
                    System.out.println(executableDirectory);
                }
                System.out.println("  " + serviceInfo.get("ExecutableTask") + ", Last run: " + serviceInfo.get("LastStartTime") + ", " + serviceInfo.get("ServiceName"));
            }
        }
    }

}
