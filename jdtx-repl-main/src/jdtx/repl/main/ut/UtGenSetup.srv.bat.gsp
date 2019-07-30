@echo off

rem серверные конфиги

rem rename %cd%\web\WEB-INF\cfg\sample.srv.json srv.json

rem del %cd%\web\WEB-INF\_app.rt

rem rename %cd%\web\WEB-INF\sample.srv._app.rt _app.rt


rem удаление старого - служба
call jc repl-service-remove -csc

rem удаление старого - данные
rmdir /Q /S %cd%\web\WEB-INF\data


rem рабочая станция
call jc repl-create -ws:${args.ws_list[0].ws_no} -guid:${args.repl_guid}-${args.ws_list[0].ws_guid}


rem сервер


<% for (int i = 0; i < args.ws_list.size; i++) { %>
call jc repl-add-ws -ws:${args.ws_list[i].ws_no} -guid:${args.repl_guid}-${args.ws_list[i].ws_guid} -name:"${args.ws_list[i].ws_name}"
<% } %>

<% for (int i = 0; i < args.ws_list.size; i++) { %>
call jc repl-enable -ws:${args.ws_list[i].ws_no}
<% } %>


rem инициализация структуры БД
call jc repl-dbstruct-finish


rem почта
call jc repl-mail-check -create:true


rem служба
call jc repl-service-install


rem для сведения
call jc repl-info

