## Kannappan compare counts
#This project is a simple JAVA WEB application
#This is suitable to run in cloud run service
#This code/project is to validate counts between two sources

Source1: GCP-BQ-Table
	>>Execute BQ query suing bigquery options service
	>>Input-->BQ query
	>>Output-->Table Result format-->Convert to numbers
	
Source2: Spluk
	>>Execute a search query in SPLUK reporting tool using splunk-API
	>>Input-->Search query
	>>Output-->Count/numbers
	
Compare counts between two sources. Print an application log message about the output.

Yet to Develop:::
	>>Create logging matrics and trigger a notification mail

Feel free to contact KANNAPPANMKCE@GMAIL.COM for any queries.