**SOLUTION**
The solution I chose focus more in performance and flexibility. I wanted the code to be resilient to schema change and inconsistencies.
In a performance perspective, I decided to have a pyspark code doing the data load and use pandas/polars flexibility to perform transformations or add calculations.

The solution runs in a container to take care of all the dependencies and runtime versions for spark - I faced a few issues with java version using conda so this sorts out complications to whoever wants to run this code. 

Having more time I'd focus more in data quality - validating each column, accomodating the problematic data in a different location and applying a repo of rules to fix and/or applying default values so the data can at least be used to count incidences or affect overall health indictors I might come up with in the future.

Tradeoffs I suppose is the relative complexity of the solution (it's not just a SQL/python script). so maintenance by another team - if such a project grows considerably, can be a challenge. The size of the image is quite considerable: 1.78Gb, but I've coming from 2.8Gb so not too bad.

Also, for performance reasons, spark can be configured better so we take advantage of its paralelism. I'm thinking of having a script splitting a huge JSON into multiple files so spark can be tuned to perform better in such scenario.

