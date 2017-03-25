require("babel-core/polyfill");
var Reflux = require("reflux");
var debounce = require("debounce");
var API = require("../utils/QueriesAPI");

export var Actions = Reflux.createActions([
    "reloadQueries",
]);

export var store = Reflux.createStore({
    init: function () {
        this.queries = [];
        this.triggerDebounced = debounce(this.trigger, 200);
        this.listenToMany(Actions);
    },

    getInitialState: function () {
        return this.queries;
    },

    onReloadQueries: function () {
        var self = this;
        API.list_queries().success(function(data){
            self.triggerDebounced(data.queries);
        });
    }
});
