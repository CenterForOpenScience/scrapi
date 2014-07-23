// This is a simple *viewmodel* - JavaScript that defines the data and behavior of your UI

var AppViewModel = new function()
{
	var count = 0;
    var article_count = 0;
    var self = this;

    self.Article = ko.observableArray();
	self.keyword = ko.observable("");

    self.keyword.subscribe(function(newValue) {
        console.log('onSubscribedActionThingy');
        console.log(newValue);
        console.log(self.Article().length);
        if (0 < self.Article().length){
            self.Article([]);
            count = 0;
            article_count = 0;
        }
        else{
            console.log("Empty article list!");
        }
    });

    self.populate_results = function()
    {
        console.log('populating: ' + self.keyword());
        //TODO universal url for live and testing
    	$.get("http://173.255.232.219/api/search?q="+self.keyword(), function(data){
        
        //$.get("http://localhost/api/search?q="+self.keyword()+"&from="+count, function(data){
            if(data != "[]"){

                var item = "";
                var returnToJson = $.parseJSON(data);

                article_count = returnToJson.length;
                
                for(var i = 0; i < returnToJson.length; i++){
                    for (var j = 0; j < returnToJson[i]["contributors"].length-1; j++){
                        item += returnToJson[i]["contributors"][j]["full_name"]+", ";
                    }
                    item += returnToJson[i]["contributors"][returnToJson[i]["contributors"].length-1]["full_name"];

                    self.Article.push(
                        {
                            title: returnToJson[i]["title"],
                            contributors: item,
                            article_id:returnToJson[i]["id"],
                            source:returnToJson[i]["source"]
                        }
                    );
                    item = "";
                }
            }
    	});
    }

    $(window).scroll(function(){ 
        if ($(window).scrollTop() == ($(document).height() - $(window).height())){
            console.log(article_count);
            count += 10;
            //count += Math.min(10, article_count);
            $('.btn').click();
        }
    })
};
// Activates knockout.js
ko.applyBindings(AppViewModel);
