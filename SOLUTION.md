**SOLUTION**
The solution I chose focus more in performance and flexibility. I wanted the code to be resilient to schema change and inconsistencies.
In a performance perspective, I decided to have a pyspark code doing the data load and use pandas/polars flexibility to perform transformations or add calculations.

we read both files (I simplify and have it fixed: vendor_a is a json and vendor_b is a csv).

For the JSON I flatten it so the solution allows flexibility for the file schema, as long as as element is present, we'll search for the first occurence of the element.

The CSV file is more straight-forward: we load all field into a df.

We scan the data using spark and in a real-life scenario this solution would need to be fine-tuned to work properly in paralell. We could split the json into smaller files, by vendor/day/campaign or in a more granular way.

While we are working with the df, we validate formats of the timestamps and other fields. We add a vendor ID into the fiel to keep track of the data lineage, we add an ETL UTC timestamp to know data and we drop dul=plicates.

The last part I calculate the KPIs for the "reports", which are: 
 - Totals by Campaign
 - Totals by Vendor
 - Totals by Day/Vendor
 - Count of Invalid Dates

The output is then saved in folder /Sample_data/reports/



The solution runs in a container to take care of all the dependencies and runtime versions for spark - I faced a few issues with java version using conda so this sorts out complications to whoever wants to run this code. 

Having more time I'd focus more in data quality - validating each column, accomodating the problematic data in a different location and applying a repo of rules to fix and/or applying default values so the data can at least be used to count incidences or affect overall health indictors I might come up with in the future.

Tradeoffs I suppose is the relative complexity of the solution (it's not just a SQL/python script). so maintenance by another team - if such a project grows considerably, can be a challenge. The size of the image is quite considerable: 1.78Gb, but I've coming from 2.8Gb so not too bad.

Also, for performance reasons, spark can be configured better so we take advantage of its paralelism. I'm thinking of having a script splitting a huge JSON into multiple files so spark can be tuned to perform better in such scenario.

Logs are gathered throughout the execution and saved in location Sample_data/logs/