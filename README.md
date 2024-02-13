Script to read an Azure Dead Letter Queue and copy the contents to a database table for easy analysis. 

I built this script for an Azure Function for a recent project I was on. 

At that point Azures DLQ interface was constantly in preview and offered not a lot in the way of easy functionality when wanting to look at tens of thousands of errors. 

So I built this script to feed the data into a database table so we could use SQL to quickly analyse the errors. 

The script works using a GET endpoint, and is built to handle a DLQ that exists in multiple environments with multiple topics (queues).

It also inserts information into a table with a timestamp, it does not wipe the table before hand but it would be easy to change if you only ever wanted to see point in time errors. 

I've genericised the script to work for anyone, hopefully hasn't implemented any bugs.

You will need to load your enviroment connection strings into Azures environment variables. 
