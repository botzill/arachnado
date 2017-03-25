var React = require("react");
var Reflux = require("reflux");
var {Table, Button, ButtonGroup} = require("react-bootstrap");
var QueriesStore = require("../stores/QueriesStore");


var Header = React.createClass({
    render() {
        return (
            <div>
                <h2>Searched Queries</h2>
            </div>
        );
    }
});

var QueriesTable = React.createClass({
    componentDidMount() {
        QueriesStore.Actions.reloadQueries();
    },
    render() {
        var rows = this.props.queries.map((query, index) =>
            <QueryRow query={query} key={query.search_term}/>
        );
        return (<Table>
            <thead>
            <tr>
                <th>Query</th>
                <th>Query Count</th>
                <th>Results</th>
                <th>Emails</th>
                <th>Crawler</th>
                <th>Download CSV(full)</th>
                <th>Download CSV(emails)</th>
            </tr>
            </thead>
            {rows}
        </Table>);
    }
});


var QueryRow = React.createClass({
    getInitialState() {
        return {
            query: this.props.query
        }
    },
    render() {
        return (
            <tbody>
            <tr>
                <td>
                    {this.props.query.search_term}
                </td>
                <td>
                    {this.props.query.search_count}
                </td>
                <td>
                    {this.props.query.results}
                </td>
                <td>
                    {this.props.query.emails}
                </td>
                <td>
                    {this.props.query.spiders[0]}
                </td>
                <td>
                    <ButtonGroup style={{display: "flex"}} bsSize="xsmall">
                        <Button bsStyle="success"
                                onClick={this.DownloadResultsFull}>Download</Button>
                    </ButtonGroup>
                </td>
                <td>
                    <ButtonGroup style={{display: "flex"}} bsSize="xsmall">
                        <Button bsStyle="success"
                                onClick={this.DownloadResultsEmails}>Download</Button>
                    </ButtonGroup>
                </td>
            </tr>
            </tbody>
        )
    },
    DownloadResultsFull(e) {
        e.preventDefault();
        window.location = window.QUERIES_DOWNLOAD_URL + '?full=1&search_term=' + this.props.query.search_term;
    },
    DownloadResultsEmails(e) {
        e.preventDefault();
        window.location = window.QUERIES_DOWNLOAD_URL + '?search_term=' + this.props.query.search_term;
    },
});

export var QueriesPage = React.createClass({
    mixins: [
        Reflux.connect(QueriesStore.store, "queries"),
    ],
    render: function () {
        return (
            <div>
                <Header/>
                <QueriesTable queries={this.state.queries}/>
            </div>
        );
    }
});
