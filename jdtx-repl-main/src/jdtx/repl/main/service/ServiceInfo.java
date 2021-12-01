package jdtx.repl.main.service;

import java.util.*;

public class ServiceInfo extends HashMap<String, String> {

    public ServiceInfo(List<String> lst) {
        super();

        for (String s : lst) {
            parse(s, "ServiceName", "Имя задачи:");
            parse(s, "ExecutableTask", "Задача для выполнения:");
            parse(s, "ExecutableDirectory", "Рабочая папка:");
            parse(s, "LastStartTime", "Время прошлого запуска:");
            parse(s, "NextStartTime", "Время следующего запуска:");
            parse(s, "UserName", "Запуск от имени:");
        }

        //
        put("ServiceName", get("ServiceName").substring(1));
    }

    private void parse(String s, String key, String keyOriginal) {
        if (s.contains(keyOriginal)) {
            this.put(key, s.split(keyOriginal)[1].trim());
        }
    }

    public String getServicePath() {
        return get("ExecutableDirectory") + get("ExecutableTask");
    }

}
