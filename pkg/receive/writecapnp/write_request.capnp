using Go = import "/go.capnp";
@0x85d3acc39d94e0f8;

$Go.package("writecapnp");
$Go.import("writecapnp");

struct Symbols {
    data @0 :Data;
    offsets @1 :List(UInt32);
}

struct Label {
    name @0 :UInt32;
    value @1 :UInt32;
}

struct Sample {
    timestamp @0 :Int64;
    value @1 :Float64;
}

struct BucketSpan {
  offset @0 :Int32;
  length @1 :UInt32;
}

struct Histogram {
    enum ResetHint {
        unknown @0;
        yes     @1;
        no      @2;
        gauge   @3;
    }

    count :union {
        countInt @0 :UInt64;
        countFloat @1 :Float64;
    }

    sum @2 :Float64;
    schema @3 :Int32;
    zeroThreshold @4 :Float64;

    zeroCount :union {
      zeroCountInt @5 :UInt64;
      zeroCountFloat @6 :Float64;
    }

    negativeSpans @7 :List(BucketSpan);
    negativeDeltas @8 :List(Int64);
    negativeCounts @9 :List(Float64);

    positiveSpans @10 :List(BucketSpan);
    positiveDeltas @11 :List(Int64);
    positiveCounts @12 :List(Float64);

    resetHint @13 :ResetHint;

    timestamp @14 :Int64;
}

struct Exemplar {
    labels @0 :List(Label);
    value @1 :Float64;
    timestamp @2 :Int64;
}

struct TimeSeries {
    labels @0 :List(Label);
    samples @1 :List(Sample);
    histograms @2: List(Histogram);
    exemplars @3: List(Exemplar);
}

struct WriteRequest {
    symbols @0: Symbols;
    timeSeries @1 :List(TimeSeries);
    tenant @2: Text;
}

enum WriteError {
    none @0;
    unavailable @1;
    alreadyExists @2;
    invalidArgument @3;
    internal @4;
}

interface Writer {
	write @0 (wr :WriteRequest) -> (error :WriteError);
}

