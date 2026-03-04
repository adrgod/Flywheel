**DESIGN**

The data schema I chosed for this scendario considers the main importatnt fields and we don't keep track of the fields that don't add to the business need.

If such project exists in another context, landing in a data lake for instance, we could consider bringing all the fields and make them vailable in the data lake and from there this or other processes could use them.

## Schema

For now, we have:

ID
Vendor
Campaign Name
Platform
Status
Region
Age Group
Impressions
Clicks
Conversions
Spend
Event Timestamp
Etl_utc_timstamp

For this project I'm only counting 
number of clicks, 
number of conversions, 
how much was spend

I'm not applying any logic based on campaign status or audience details. This could be an improvement, if business requires.

I'm considering all currencies are the same: USD, no conversions applied.

In terms of dates and timestamps, I use and consider on UTC. We have an event timestamp from source and I'm adding the ETL process timestamp to the output to know when a row was loaded.

## Partitioning

It's hard to make a good decision regarding partitioning when I don't have a real sense of what the volume and frequency is. But assuming we have two large files, partitioning by time or vendor or campaign are good candidates.

I'm not splitting the source files before consuming it with spark, but that can be done if volume is very big. 

I'm consuming both files as they are and partition spark output by event date.

To consume that parquet and perform calculations, I read the data in "processed" folder, apply the calculations and output the result in /reports folder.
