### jdtx.repl.main.api.database_info.DatabaseInfoReaderService

доделать ИСПОЛЬЗОВАНИЕ именно через СЕРВИС, убрать все конструкторы, убрать фабрику

### jdtx.repl.main.api.rec_merge.GroupsStrategyStorage.initInstance

initInstance На очередь в рефакторинг

### Разнобой в определении возраста

long queDoneNo1 = queInSrv.getMaxNo();

```
long queDoneNo = stateManager.getWsQueInNoReceived(wsId);
long mailMaxNo = mailerWs.getBoxState("from");

А почему не так определять?
long queDoneNo1 = queInSrv.getMaxNo();

stateManager.getWsQueInNoReceived выполняет такой же запрос 
select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE where ws_id = " + wsId

Путаница при отметке возраста
```

### jdtx.repl.main.api.JdxReplWs.replicasSendDir

зарефакторить метод, чтобы было удобно и управляемо как в методе receiveQueInternalStep(mailer, box, no, que)

### jdtx.repl.main.ext.Jdx_Ext.repl_ws_mute

почему нет команды, чтобы это сделать прямо на рабочей станции (с отчетом на сервер)? проверить, чтобы все команды по
настройке станции (send***) имели аналог на самой станции (с отчетом на сервер)

### направления развития

1) меняем rootDir и создаем каталог для орпакла, тестируем baseReplication& только надо сделать приавильный "изменятель
   данных" из закромов
2) интерфейс (web или командная строка) настройки репликации

### java-as-service

https://stackoverflow.com/questions/68113/how-to-create-a-windows-service-from-java-app
https://commons.apache.org/proper/commons-daemon/procrun.html
https://downloads.apache.org/commons/daemon/binaries/windows/



