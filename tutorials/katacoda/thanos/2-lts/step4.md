### TODO: Show that queries are working, served by Thanos Store Gateway.

Downsampling is the most important and required feature - it’s the ability to keep long term metrics with fewer number of samples.

If we don't perform that querying metrics with a high number of time series might cause issues because of the amount of data fetched by store.