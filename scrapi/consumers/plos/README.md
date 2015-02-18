PLoS-API-consumer
=================
<p>To run "consumer.py" please follow the instructions:</p>
<ol>

<li>Create an account on <a href="http://register.plos.org/ambra-registration/register.action">PLOS API</a></li>

<li>Sign in <a href="http://alm.plos.org/">here</a> and click on your account name. Retrieve your API key.</li>

<li>Create a new file in the folder named "settings.py". In the file, put<br>
<code>API_KEY = (your API key)</code></li>

</ol>

Sample API query: http://api.plos.org/search?q=publication_date:[2015-01-30T00:00:00Z%20TO%202015-02-02T00:00:00Z]&api_key=ayourapikeyhere&rows=999&start=0
