;(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['knockout', 'jquery', 'knockoutpunches'], factory);
    } else {
        global.ShareSearch  = factory(ko, jQuery);
    }
}(this, function(ko, $) {
    // Enable knockout punches
    ko.punches.enableAll();

    var ViewModel = function(url) {
        var self = this;

        self.queryUrl = url;
        self.totalResults = ko.observable(0);
        self.resultsPerPage = ko.observable(10);
        self.currentPage = ko.observable(1);
        self.query = ko.observable('');
        self.results = ko.observableArray([]);

        self.totalPages = ko.computed(function() {
            var pageCount = 1;
            var resultsCount = Math.max(self.resultsPerPage(),1); // No Divide by Zero
            pageCount = Math.ceil(self.totalResults / resultsCount);
            return pageCount;
        });

        self.nextPageExists = ko.computed(function() {
            return self.totalPages() > 1 && self.currentPage() < self.totalPages();
        });

        self.prevPageExists = ko.computed(function() {
            return self.totalPages() > 1 && self.currentPage() > 1;
        });

        self.currentIndex = ko.computed(function() {
            return self.resultsPerPage() * self.currentPage()
        });

        self.fullQuery = ko.computed(function() {
            var queryString = "?q=" + self.query();
            queryString += "&start=" + self.currentIndex();
            queryString += "&size=" + self.resultsPerPage();
            return queryString;
        });

        self.search = function() {
            $.ajax({
                url: self.queryUrl() + '?q=' + self.query() + '&start=' + self.currentIndex(),
                type: 'GET',
                success: self.searchRecieved
            });
        };

        self.pageNext = function() {
            self.currentIndex(self.currentIndex() + 1);
            self.search();
        };

        self.pagePrev = function() {
            self.currentIndex(self.currentIndex() - 1);
            self.search();
        };

        self.getMetadata = function(id) {
            $.ajax({
                url: self.queryUrl + id,
                type: 'GET',
                success: self.metadataRecieved
            });
        };

        self.searchReceived = function(data) {
            self.totalResults(data.total);
            self.results(data.results);
        };

        self.metadataReceived = function(data) {
            self.metadata('\n' + jsl.format.formatJson(JSON.stringify(data)));
        };

        self.setSelected = function(datum) {
            self.chosen(datum);
            self.getMetadata(datum.guid)
        };

    };

    function ShareSearch(selector, url) {
        // Initialization code
        var self = this;
        self.viewModel = new ViewModel(url);
        ko.applyBindings(self.viewModel, selector);
    }

    return ApplicationView

}));