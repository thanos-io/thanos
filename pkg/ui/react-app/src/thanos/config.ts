declare const THANOS_QUERY_URL: string;

export let queryURL = THANOS_QUERY_URL;
if (!queryURL) {
  queryURL = 'http://localhost:10902';
}
