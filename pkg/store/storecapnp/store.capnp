using Go = import "/go.capnp";
@0xb8e4c1f0a2d3e5f7;

$Go.package("storecapnp");
$Go.import("github.com/thanos-io/thanos/pkg/store/storecapnp");

# Symbol table for efficient string deduplication
struct Symbols {
    data @0 :Data;
    offsets @1 :List(UInt32);
}

# Label is a name/value pair using indices into the symbol table
struct Label {
    name @0 :UInt32;
    value @1 :UInt32;
}

# LabelMatcher specifies a rule for matching labels
struct LabelMatcher {
    enum Type {
        eq @0;   # =
        neq @1;  # !=
        re @2;   # =~
        nre @3;  # !~
    }
    type @0 :Type;
    name @1 :UInt32;   # Index into symbols
    value @2 :UInt32;  # Index into symbols
}

# Chunk encoding types
enum ChunkEncoding {
    xor @0;
    histogram @1;
    floatHistogram @2;
}

# Chunk contains encoded time series data
struct Chunk {
    type @0 :ChunkEncoding;
    data @1 :Data;
    hash @2 :UInt64;
}

# AggrChunk contains aggregated chunk data for downsampling
struct AggrChunk {
    minTime @0 :Int64;
    maxTime @1 :Int64;
    raw @2 :Chunk;
    count @3 :Chunk;
    sum @4 :Chunk;
    min @5 :Chunk;
    max @6 :Chunk;
    counter @7 :Chunk;
}

# Series contains labels and chunks for a single time series
struct Series {
    labels @0 :List(Label);
    chunks @1 :List(AggrChunk);
}

# Aggregation type for downsampled data
enum Aggr {
    raw @0;
    count @1;
    sum @2;
    min @3;
    max @4;
    counter @5;
}

# PartialResponseStrategy controls how partial responses are handled
enum PartialResponseStrategy {
    warn @0;
    abort @1;
}

# Func represents a PromQL function hint
struct Func {
    name @0 :UInt32;  # Index into symbols
}

# Grouping represents grouping expression hints
struct Grouping {
    by @0 :Bool;
    labels @1 :List(UInt32);  # Indices into symbols
}

# Range represents range vector selector hint
struct Range {
    millis @0 :Int64;
}

# QueryHints provides hints from PromQL for optimization
struct QueryHints {
    stepMillis @0 :Int64;
    func @1 :Func;
    grouping @2 :Grouping;
    range @3 :Range;
}

# ShardInfo for sharding series across stores
struct ShardInfo {
    shardIndex @0 :Int64;
    totalShards @1 :Int64;
    by @2 :Bool;
    labels @3 :List(UInt32);  # Indices into symbols
}

# SeriesRequest is the request for streaming series data
struct SeriesRequest {
    symbols @0 :Symbols;
    minTime @1 :Int64;
    maxTime @2 :Int64;
    matchers @3 :List(LabelMatcher);
    maxResolutionWindow @4 :Int64;
    aggregates @5 :List(Aggr);
    partialResponseDisabled @6 :Bool;
    partialResponseStrategy @7 :PartialResponseStrategy;
    skipChunks @8 :Bool;
    hints @9 :Data;  # Opaque hints data (Any proto)
    step @10 :Int64;  # Deprecated
    range @11 :Int64;  # Deprecated
    queryHints @12 :QueryHints;
    shardInfo @13 :ShardInfo;
    withoutReplicaLabels @14 :List(UInt32);  # Indices into symbols
    limit @15 :Int64;
}

# SeriesResponse is a single frame in the series stream
struct SeriesResponse {
    symbols @0 :Symbols;
    union {
        series @1 :Series;
        warning @2 :Text;
        hints @3 :Data;  # Opaque hints data (Any proto)
    }
}

# LabelNamesRequest requests label names
struct LabelNamesRequest {
    symbols @0 :Symbols;
    partialResponseDisabled @1 :Bool;
    partialResponseStrategy @2 :PartialResponseStrategy;
    start @3 :Int64;
    end @4 :Int64;
    hints @5 :Data;
    matchers @6 :List(LabelMatcher);
    withoutReplicaLabels @7 :List(UInt32);
    limit @8 :Int64;
}

# LabelNamesResponse contains label names
struct LabelNamesResponse {
    symbols @0 :Symbols;
    names @1 :List(UInt32);  # Indices into symbols
    warnings @2 :List(Text);
    hints @3 :Data;
}

# LabelValuesRequest requests values for a specific label
struct LabelValuesRequest {
    symbols @0 :Symbols;
    label @1 :UInt32;  # Index into symbols
    partialResponseDisabled @2 :Bool;
    partialResponseStrategy @3 :PartialResponseStrategy;
    start @4 :Int64;
    end @5 :Int64;
    hints @6 :Data;
    matchers @7 :List(LabelMatcher);
    withoutReplicaLabels @8 :List(UInt32);
    limit @9 :Int64;
}

# LabelValuesResponse contains label values
struct LabelValuesResponse {
    symbols @0 :Symbols;
    values @1 :List(UInt32);  # Indices into symbols
    warnings @2 :List(Text);
    hints @3 :Data;
}

# StoreError represents errors from the store
enum StoreError {
    none @0;
    unavailable @1;
    invalidArgument @2;
    notFound @3;
    internal @4;
    resourceExhausted @5;
}

# LabelSet is a set of labels
struct LabelSet {
    labels @0 :List(Label);
}

# TSDBInfo contains information about a TSDB
struct TSDBInfo {
    labels @0 :LabelSet;
    minTime @1 :Int64;
    maxTime @2 :Int64;
}

# StoreInfo contains information about a store's capabilities and data range
struct StoreInfo {
    minTime @0 :Int64;
    maxTime @1 :Int64;
    supportsSharding @2 :Bool;
    supportsWithoutReplicaLabels @3 :Bool;
    tsdbInfos @4 :List(TSDBInfo);
}

# InfoRequest requests store information
struct InfoRequest {
    # Empty for now, but allows future extension
}

# InfoResponse contains store metadata
struct InfoResponse {
    symbols @0 :Symbols;
    labelSets @1 :List(LabelSet);
    storeInfo @2 :StoreInfo;
}

# Store interface for querying time series data
interface Store {
    # Info returns metadata about the store
    info @0 (req :InfoRequest) -> (resp :InfoResponse, error :StoreError, errorContext :Text);

    # Series streams series data for the given request
    series @1 (req :SeriesRequest) -> (stream :SeriesStream);
    
    # LabelNames returns all label names
    labelNames @2 (req :LabelNamesRequest) -> (resp :LabelNamesResponse, error :StoreError, errorContext :Text);
    
    # LabelValues returns values for a given label
    labelValues @3 (req :LabelValuesRequest) -> (resp :LabelValuesResponse, error :StoreError, errorContext :Text);
}

# SeriesStream is a streaming interface for series responses
interface SeriesStream {
    # Next returns the next series response, or sets done=true when complete
    next @0 () -> (resp :SeriesResponse, done :Bool, error :StoreError, errorContext :Text);
}
