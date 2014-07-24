// This is a simple *viewmodel* - JavaScript that defines the data and behavior of your UI
$(".loader").hide();
$("#no-results").hide();

var AppViewModel = new function()
{
	var count = 0;
    var article_count = 0;
    var self = this;

    self.Article = ko.observableArray();
	self.keyword = ko.observable("");

    self.keyword.subscribe(function(newValue) {
        if (0 < self.Article().length){
            $(".loader").show();
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
        //TODO universal url for live and testing
    	$.get("http://173.255.232.219/api/search?q="+self.keyword()+"&from="+count, function(data){
        //$.get("http://localhost/api/search?q="+self.keyword()+"&from="+count, function(data){
            if(data != "[]"){

                var contributors_list = "";
                var properties_list = "";

                var returnToJson = $.parseJSON(data);
                console.log(returnToJson);
                
                article_count = returnToJson.length;
                
                for(var i = 0; i < returnToJson.length; i++){
                    for (var j = 0; j < returnToJson[i]["contributors"].length-1; j++){
                        contributors_list += returnToJson[i]["contributors"][j]["full_name"]+"; ";
                    }
                    contributors_list += returnToJson[i]["contributors"][returnToJson[i]["contributors"].length-1]["full_name"];

                    for (property in returnToJson[i]["properties"]){
                        if(returnToJson[i]["properties"][property] == null || returnToJson[i]["properties"][property] == ""){
                            continue;
                        }
                        else{
                            properties_list += "<p><strong>" + property + ": </strong>" + returnToJson[i]["properties"][property]+"</p>";
                        }
                    }

                    //console.log(properties_list);

                    $(".loader").hide();

                    self.Article.push(
                        {
                            title: returnToJson[i]["title"],
                            contributors: contributors_list,
                            article_id:returnToJson[i]["id"],
                            source:returnToJson[i]["source"],
                            properties:properties_list
                        }
                    );

                    contributors_list = "";
                    properties_list = "";
                }
            }else{
                $(".loader").hide();
                $("#no-results").show();
            }
    	});
    }

    $(window).scroll(function(){ 
        if ($(window).scrollTop() == ($(document).height() - $(window).height())){
            $(".loader").show();
            //count += 10;
            count += Math.min(10, article_count);
            $('.btn').click();
        }
    })

    $(".search-results").on("click", ".jsonObject", function(e){
        $(e.target).closest('.jsonObject').toggleClass('collapsed');
        //var file_name = $(e.target).text();
        //console.log(file_name);
    });
};
// Activates knockout.js
ko.applyBindings(AppViewModel);
