<% for (sss in args.p2) { %>
p2: ${sss}
<% } %>

@echo off
call jc repl-service-remove



rem рабочая станция
call jc repl-create -ws:${args.ws_list[0].ws_no} -guid:${args.repl_guid}-${args.ws_list[0].ws_guid}



rem сервер


<% for (int i = 0; i < args.ws_list.size; i++) { %>
call jc repl-add-ws -ws:${args.ws_list[i].ws_no} -guid:${args.repl_guid}-${args.ws_list[i].ws_guid} -name:"${args.ws_list[i].ws_name}"
<% } %>

<% for (int i = 0; i < args.ws_list.size; i++) { %>
call jc repl-enable -ws:${args.ws_list[i].ws_no}
<% } %>



rem почта
call jc repl-mail-check -create:true



rem служба
call jc repl-service-install



rem для сведения
call jc repl-info

