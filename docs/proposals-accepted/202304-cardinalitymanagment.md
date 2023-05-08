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

We will be developing a new API in Thanos to calculate series cardinality through Thanos Receiver.

#### API Design

The new API will have the following endpoint-

`GET,POST /api/v1/cardinality`

It will retrieve the series cardinality information.

The API will accept the following query parameters:

- topK : (optional) An integer specifying the top K results to return for each category. Default is 10.
- selector : (optional) A PromQL selector that will be used to filter series that must be analyzed.
- focusLabel : (optional) A string representing the label name for which you want to retrieve the series count by its unique values. When provided, the API response will include the `seriesCountByFocusLabelValue` field, containing the unique values for the specified label and their corresponding series counts.

The API response will contain

- `totalSeries`: Total number of series.
- `totalLabelValuePairs`: Total number of label-value pairs.
- `seriesCountByMetricName`: List of objects containing metric name and its corresponding series count.
- `seriesCountByLabelName`: List of objects containing label name and its corresponding series count.
- `seriesCountByFocusLabelValue`: List of objects containing focus label-value and its corresponding series count.
- `seriesCountByLabelValuePair`: List of objects containing label-value pair and its corresponding series count.
- `labelValueCountByLabelName`: List of objects containing label name and its corresponding label value count.

Let us take a look at the API response with some examples.

Example metrics:
```
{__name__="metricA", instance="A", cluster="us"}
{__name__="metricA", instance="B", cluster="eu"}
{__name__="metricB", instance="C", cluster="us"}
```

Example 1: Request without any parameters.

`GET,POST /api/v1/cardinality`

Then, API response would be

```{
  "status": "success",
  "data": {
    "totalSeries": 3,
    "totalLabelValuePairs": 7,
    "seriesCountByMetricName": [
      { "name": "metricA", "value": 2 },
      { "name": "metricB", "value": 1 }
    ],
    "seriesCountByLabelName": [
      { "name": "cluster", "value": 3 },
      { "name": "instance", "value": 3 }
    ],
    "seriesCountByLabelValuePair": [
      { "name": "__name__=metricA", "value": 2 },
      { "name": "cluster=us", "value": 2 },
      { "name": "__name__=metricB", "value": 1 }
      { "name": "cluster=eu", "value": 1 },
      { "name": "instance=A", "value": 1 },
      { "name": "instance=B", "value": 1 },
      { "name": "instance=C", "value": 1 },
    ],
    "labelValueCountByLabelName": [
      { "name": "cluster", "value": 2 },
      { "name": "instance", "value": 3 }
    ],
    "seriesCountByFocusLabelValue": []
  }
}

```

Example 2: Request with topK=2 and focusLabel=cluster.

` GET,POST /api/v1/cardinality?topK=2&focusLabel=cluster`

Then, API response would be

```{
"status": "success",
"data": {
  "totalSeries": 3,
  "totalLabelValuePairs": 5,
  "seriesCountByMetricName": [
    { "name": "metricA", "value": 2 },
    { "name": "metricB", "value": 1 }
  ],
  "seriesCountByLabelName": [
    { "name": "cluster", "value": 3 },
    { "name": "instance", "value": 3 }
  ],
  "seriesCountByLabelValuePair": [
    { "name": "__name__=metricA", "value": 2 },
    { "name": "cluster=us", "value": 2 },
  ],
  "labelValueCountByLabelName": [
    { "name": "cluster", "value": 2 },
    { "name": "instance", "value": 3 }
  ],
  "seriesCountByFocusLabelValue": [
    { "name": "us", "value": 2 },
    { "name": "eu", "value": 1 }
  ]
  ]
}
}
```

Example 3 : Request with selector={__name__="metricA"}.

`GET,POST /api/v1/cardinality?selector={__name__="metricA"}`

Then, API response would be

```
{
  "status": "success",
  "data": {
    "totalSeries": 2,
    "totalLabelValuePairs": 3,
    "seriesCountByMetricName": [
      { "name": "metricA", "value": 2 }
    ],
    "seriesCountByLabelName": [
      { "name": "cluster", "value": 2 },
      { "name": "instance", "value": 2 }
    ],
    "seriesCountByLabelValuePair": [
      { "name": "__name__=metricA", "value": 2 },
      { "name": "cluster=us", "value": 1 },
      { "name": "cluster=eu", "value": 1 },
      { "name": "instance=A", "value": 1 },
      { "name": "instance=B", "value": 1 },
    ],
    "labelValueCountByLabelName": [
      { "name": "cluster", "value": 2 },
      { "name": "instance", "value": 2 }
    ],
    "seriesCountByFocusLabelValue": []
  }
}
```

#### Implementation

For now extending receiver, seems the best approach to use. And, exact implementation can change while implementing the proposed API design.

1. We will first access and aggregate the series, labels, and label-value pairs information within the Thanos receiver by extending its functionality while processing incoming data.

2. There will be a new data structure to hold the statistics. This structure should have methods to compute the required statistics.
3. We will define a new HTTP endpoint in the Thanos receiver to expose the API, as `/api/v1/cardinality`.

4. We will Implement a new API handler function that

   - Accepts optional query parameters, such as top, for filtering and sorting the results.
   - Calls the data structure's methods to compute the required statistics.
   - Serializes the results into JSON format and returns it as an HTTP response.
   - Handles potential partial results by setting the isPartial field in the response and providing an appropriate error message.

5. (Optional)We can add a flag to the Thanos receiver, allowing users to enable or disable the API.

### Alternatives

#### Two seperate APIs

- We can also perform another API design, where we will have two seperate endpoints for label names cardinality and label values cardinality.Let us take a look at it.

##### Label Names

`GET,POST /api/v1/cardinality/label_names`

This endpoint will be responsible for returning the realtime label names cardinality . It will count distinct label values per label name.

Request parameters:

- selector - (optional) specifies PromQL selector that will be used to filter series that must be analyzed.
- limit - (optional) specifies max count of items in field cardinality in response (default=20, min=0, max=500)

Response -

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

##### Label values

This endpoint will return the label values cardinality associated with request parameter `label_names[]`. It will return the series count per label value for each label in the request parameter `label_names[]`.

`GET,POST /api/v1/cardinality/label_values`

Request parameters:

- label_names[] - (required) specifies label names for which label values cardinality will be returned.
- selector - (optional) specifies PromQL selector that will be used to filter series that must be analyzed.
- limit - (optional) specifies max count of items in field cardinality in response (default=20, min=0, max=500).

```
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

#### Pros

1. It provides more granularity about series data. Also, we can get specific information that we need.Users can choose to query label names, label values, or both depending on their requirements.
2. It can be more efficient as we query only the data that we need.

#### Cons

1. It will be more complex to users as they will have to query two endpoints to get the required information.


