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

Currently, Thanos receiver supports the TSDB stats API on remote write addresses, which provides information about series cardinality. However, this API some limitations, it does not provide series count for a specific label and cardinality information for particular matcher.

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
- matcher : (optional) A matcher that will be used to filter series that must be analyzed.
- focusLabel : (optional) A string representing the label name for which you want to retrieve the series count by its unique values. When provided, the API response will include the `seriesCountByFocusLabelValue` field, containing the unique values for the specified label and their corresponding series counts.

The API response will contain

- `totalSeries`: Total number of series.
- `totalLabelValuePairs`: Total number of label-value pairs.
- `seriesCountByMetricName`: List of objects containing metric name and its corresponding series count.
- `seriesCountByLabelName`: List of objects containing label name and its corresponding series count.
- `seriesCountByFocusLabel`: List of objects containing focus label-value and its corresponding series count.
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
    "seriesCountByFocusLabel": []
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
  "seriesCountByFocusLabel": [
    { "name": "us", "value": 2 },
    { "name": "eu", "value": 1 }
  ]
}
}
```

Example 3 : Request with matcher={__name__="metricA"}.

`GET,POST /api/v1/cardinality?matcher={__name__="metricA"}`

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
    "seriesCountByFocusLabel": []
  }
}
```

#### Implementation

For now extending receiver, seems the best approach to use. And, exact implementation can change while implementing the proposed API design.

1. Define & Initialize CardinalityStats Structure <br>
   Create a structure named CardinalityStats to hold the cardinality statistics and initialize it using the NewCardinalityStats function.

2. Update and Calculate Statistics for Each Series <br>
   In this step, we update the statistics for each series and calculate the cardinality. We use the UpdateStats method, which takes a set of labels and a focus label as input. For each label in the set, it increments the count of the corresponding series, label name, and label-value pair. If the label name matches the focus label, it also increments the count of the focus label value. The total number of series and total label-value pairs are also incremented.

Here's a simplified pseudo-code representation of the UpdateStats method:

```
function UpdateStats(lbls, focusLabel):
  increment totalSeries

  for each label in lbls:
      increment seriesCountByLabelName[label.Name]
      increment seriesCountByLabelValuePair[label.Name+":"+label.Value]

      if label.Name equals focusLabel:
          increment seriesCountByFocusLabelValue[label.Value]

      if label.Name equals "__name__":
          increment seriesCountByMetricName[label.Value]

      increment totalLabelValuePairs
      increment labelValueCountByLabelName[label.Name]
```

This method is called for each series in the CalculateCardinalityStats method, which iterates over the series using an index reader, applies any matchers to filter the series, and then updates the statistics for each series.

Here's a simplified pseudo-code representation of the CalculateCardinalityStats method:

```
function CalculateCardinalityStats(indexReader, focusLabel, matchers):
    get postings from indexReader

    for each posting in postings:
        get series from indexReader
        get labels from series

        for each matcher in matchers:
            if matcher matches label value:
                call UpdateStats with labels and focusLabel
```

3. Define endpoints and implement the API handler function <br>
   We define the new endpoint and implement a new API handler function that accepts optional query parameters, computes the required statistics, and serializes the results into JSON format.

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
