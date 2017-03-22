var {jsonAjax} = require("./AjaxUtils");


export function list_queries(){
    var data = {};
    return jsonAjax(window.QUERIES_LIST_URL, data);
}
