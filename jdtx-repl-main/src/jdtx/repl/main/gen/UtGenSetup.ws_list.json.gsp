[
<% for (int i = 0; i < args.ws_list.size; i++) { %>
  {
    "title": "${args.ws_list[i].ws_name}",
    "no": ${args.ws_list[i].ws_no},
    "guid": "${args.ws_list[i].ws_guid}"
  }${i == args.ws_list.size-1?"":","}
<% } %>
]
