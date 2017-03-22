require("babel-core/polyfill");
var Reflux = require("reflux");
var debounce = require("debounce");
var API = require("../utils/QueriesAPI");

export var store = Reflux.createStore({
    init: function () {
        this.queries = [];
        var self = this;
        API.list_queries().success(function(data){
            self.trigger(data.queries);
        });
    },

    getInitialState: function () {
        var queries = this.queries;
        return queries
    }
});
