// This is a simple *viewmodel* - JavaScript that defines the data and behavior of your UI
var AppViewModel = new function()
{
	var self = this;

	self.keyword = ko.observable("");
	self.response = ko.observable(null);

    self.populate_results = function()
    {
    	$.get("http://173.255.232.219/api/search?q="+self.keyword(), function(data){
    		var item = ""
    		var returnToJson = $.parseJSON(data);
    		console.log(returnToJson);
    		for(var i = 0; i < returnToJson.length; i++){
    			item += '<div class="jsonObject"><h2>';
    			item += returnToJson[i]["title"];
    			item += "</h2>";
    			item += "<p>";
    			for (var j = 0; j < returnToJson[i]["contributors"].length-1; j++){
    				item += returnToJson[i]["contributors"][j]["full_name"]+", ";
    			}
    			item += returnToJson[i]["contributors"][returnToJson[i]["contributors"].length-1]["full_name"];
    			item += "</p>";
    			item +="<p>";
    			item += '<b>ID: </b>'+returnToJson[i]["id"];
    			item +="</p>";
    			item +="<p>";
    			item += '<b>Source: </b>'+returnToJson[i]["source"]
    			item +="</p>";
    			item +="</div>"
    		}
    		
    		self.response(item);
    	});
    }
}

// Activates knockout.js
ko.applyBindings(AppViewModel);
