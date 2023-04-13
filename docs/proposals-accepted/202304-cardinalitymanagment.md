---
type: proposal
title: Series Cardinality API
status: accepted
owner: jatinagwal
menu: proposals-accepted
---

### Related links/tickets

- https://github.com/thanos-io/thanos/issues/6007
- https://mentorship.lfx.linuxfoundation.org/project/dbce5279-d029-46f3-b117-9e9dd7f84bd6

### Why

At present, multiple product teams send their metrics to Thanos. However, when product teams send metrics with high cardinality to Thanos, it can overwhelm the Store Gateway and Queriers, preventing any teams from running queries. To prevent this issue from occurring, we should have a cardinality management support in Thanos. This would allow teams to monitor the cardinality of both labels and metrics.

### Pitfalls of current solutions

Currently, Thanos receiver supports the TSDB stats API on remote write addresses, which provides information about series cardinality. However, this API can only return a limited number of results per query, typically no more than 10. This limitation makes it difficult to track arbitrary metrics and may lead to incomplete or inaccurate cardinality data. To address this issue, there should be some Top-K selector that can return the stats of the top K entries, allowing teams to more effectively monitor cardinality and identify potential issues.

### Goals

Implement APIs exposing series cardinalities in Thanos.

### Proposal

We will be Developing a New API in Thanos to calculate series cardinality through Thanos Receiver.

#### API Design

The new API will have the following endpoint:

`/api/v1/cardinality`: It will retrieve the series cardinality information.
The API response will contain

- `totalSeries`: Total number of series.
- `totalLabelValuePairs`: Total number of label-value pairs.
- `seriesCountByMetricName`: List of objects containing metric name and its corresponding series count.
- `seriesCountByLabelName`: List of objects containing label name and its corresponding series count.
- `seriesCountByFocusLabelValue`: List of objects containing focus label-value and its corresponding series count.
- `seriesCountByLabelValuePair`: List of objects containing label-value pair and its corresponding series count.
- `labelValueCountByLabelName`: List of objects containing label name and its corresponding label value count.\*

The API response will be 

```{
  "status": "success",
  "isPartial": false,
  "data": {
    "totalSeries": 14752,
    "totalLabelValuePairs": 100544,
    ... // other data fields
  }
}
```

#### Implementation

For now extending receiver, seems the best approach to use. And, exact implementation can change while implementing the proposed API design.

1. We will first access and aggregate the series, labels, and label-value pairs information within the Thanos receiver by extending its functionality while processing incoming data. The Thanos receiver ingests data and writes it to a TSDB instance.
2. There will be a new data structure to hold the statistics. This structure should have methods to

   - Add new data points (series, labels, and label-value pairs).
   - Compute the statistics required by the API.
   - Serialize the data into a JSON format according to the API design.

3. We will define a new HTTP endpoint in the Thanos receiver to expose the API,as `/api/v1/cardinality`.

4. We will Implement a new API handler function that

   - Accepts optional query parameters, such as top , for filtering and sorting the results.
   - Calls the data structure's methods to compute the required statistics.
   - Serializes the results into JSON format and returns it as an HTTP response.
   - Handles potential partial results by setting the isPartial field in the response and providing an appropriate error message.

5. (Optional)We can add a flag to the Thanos receiver, allowing users to enable or disable the API.

### Alternatives

- Alternative approach could be through Prometheus's [`/api/v1/series`](https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers) but, it not useful for large scale or large number of labels as when using the `/api/v1/series` endpoint, the Prometheus server must iterate over all the time series data to identify the series that match the label selectors in the match[] parameter. This can be a computationally intensive operation.
- We can also perform another API design, where we will have two seperate endpoints for label names cardinality and label values cardinality.Let us take a look at it

  - Label Names endpoint: This endpoint will be responsible for returning the realtime label names cardinality . It will count distinct label values per label name.<br>
    Response: <br>

  ```
  {
  "label_values_count_total": <number>,
  "label_names_count": <number>,
  "cardinality": [
    {
      "label_name": <string>,
      "label_values_count": <number>
    }
  ]
  }
  ```

  - Label values cardinality endpoint: This endpoint will return the label values cardinality associated with request parameter `label_names[]`. It will return the series count per label value for each label in the request parameter `label_names[]`.<br>

  ```proto
  {
  "series_count_total": <number>,
  "labels": [
    {
      "label_name": <string>,
      "label_values_count": <number>,
      "series_count": <number>,
      "cardinality": [
        {
          "label_value": <string>,
          "series_count": <number>
        }
      ]
    }
  ]
  }
  ```

Now, both these endpoints can have a `limit` parameter which will specify number of items in the response.

#### Pros

1. It provides more granularity about series data.Also, we can get specific information that we need.Users can choose to query label names, label values, or both depending on their requirements.
2. Users can provide optional PromQL selectors to filter series to analyze, allowing them to focus on specific metrics or series of interest.

### Cons

1. Complexity: The API's granularity can make it more complex for users to consume the data, especially when compared to a more concise API that returns all information in one call.
2. It might have a performance disadvantage when users require all the information provided by the API, as it may require multiple calls to retrieve the complete dataset.

### Open Questions

1. Both api designs are good, with which we should be going forward ?
