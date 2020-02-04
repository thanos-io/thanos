#!/usr/bin/env bash

DIR="$(dirname $(readlink -e -v ${BASH_SOURCE[0]}))"

cd ${DIR}/../../

RUN="expandedpostings"
mkdir -p ${DIR}/bench_outs/${RUN}

for TEST in BenchmarkBucketIndexReader_ExpandedPostings/binary_header BenchmarkBucketIndexReader_ExpandedPostings/index_json; do
    echo "Running ${TEST} run ${RUN}"
    go test -bench=${TEST} -run=^$ -benchmem -memprofile ${DIR}/bench_outs/${RUN}/memprofile${TEST//\//-}.out -timeout 2h -benchtime 60s ./pkg/store > ${DIR}/bench_outs/${RUN}/bench${TEST//\//-}.out
done

sed 's/BenchmarkBucketIndexReader_ExpandedPostings\/binary_header/BenchmarkBucketIndexReader_ExpandedPostings/g' ${DIR}/bench_outs/${RUN}/benchBenchmarkBucketIndexReader_ExpandedPostings-binary_header.out > ${DIR}/bench_outs/${RUN}/bench_new.out

sed 's/BenchmarkBucketIndexReader_ExpandedPostings\/index_json/BenchmarkBucketIndexReader_ExpandedPostings/g' ${DIR}/bench_outs/${RUN}/benchBenchmarkBucketIndexReader_ExpandedPostings-index_json.out > ${DIR}/bench_outs/${RUN}/bench_old.out

benchcmp ${DIR}/bench_outs/${RUN}/bench_old.out ${DIR}/bench_outs/${RUN}/bench_new.out | tee ${DIR}/bench_outs/${RUN}/bench.out
