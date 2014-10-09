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
        self.resultsPerPage = ko.observable(5);
        self.currentPage = ko.observable(1);
        self.query = ko.observable('');
        self.results = ko.observableArray([]);
        self.searching = ko.observable(false);

        self.totalPages = ko.computed(function() {
            var pageCount = 1;
            var resultsCount = Math.max(self.resultsPerPage(),1); // No Divide by Zero
            pageCount = Math.ceil(self.totalResults() / resultsCount);
            return pageCount;
        });

        self.nextPageExists = ko.computed(function() {
            return ((self.totalPages() > 1) && (self.currentPage() < self.totalPages()));
        });

        self.prevPageExists = ko.computed(function() {
            return self.totalPages() > 1 && self.currentPage() > 1;
        });

        self.currentIndex = ko.computed(function() {
            return Math.max(self.resultsPerPage() * (self.currentPage()-1),0)
        });

        self.navLocation = ko.computed(function() {
            return 'Page ' + self.currentPage() + ' of ' + self.totalPages();
        });

        self.fullQuery = ko.computed(function() {
            var queryString = "?q=" + self.query();
            queryString += "&from=" + self.currentIndex();
            queryString += "&size=" + self.resultsPerPage();
            return queryString;
        });

        self.submit = function() {
            self.currentPage(1);
            self.results.removeAll();
            self.search();
        };

        self.search = function() {
            $.ajax({
                url: self.queryUrl + self.fullQuery(),
                type: 'GET'
            }).success(function(data) {
                self.totalResults(data.total);
                self.results.removeAll();
                self.results(data.results);
            }).fail(function(){
                console.log("error");
                self.totalResults(0);
                self.currentPage(0);
                self.results.removeAll();
            });
        };

        self.pageNext = function() {
            self.currentPage(self.currentPage() + 1);
            self.search();
        };

        self.pagePrev = function() {
            self.currentPage(self.currentPage() - 1);
            self.search();
        };

        self.getMetadata = function(id) {
            $.ajax({
                url: self.queryUrl + id,
                type: 'GET',
                success: self.metadataRecieved
            });
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
        element = $(selector).get();
        ko.applyBindings(self.viewModel, element[0]);
    }

    return ShareSearch

}));