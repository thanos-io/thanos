#!/usr/bin/env bash

DIR="$(dirname $(readlink -e -v ${BASH_SOURCE[0]}))"

cd ${DIR}/../../

RUN="series"
mkdir -p ${DIR}/bench_outs/${RUN}

for TEST in BenchmarkSeries/binary_header BenchmarkSeries/index_json; do
    echo "Running ${TEST} run ${RUN}"
    go test -bench=${TEST} -run=^$ -benchmem -memprofile ${DIR}/bench_outs/${RUN}/memprofile${TEST//\//-}.out -timeout 2h -benchtime 30s ./pkg/store > ${DIR}/bench_outs/${RUN}/bench${TEST//\//-}.out
done

sed 's/BenchmarkSeries\/binary_header/BenchmarkSeries/g' ${DIR}/bench_outs/${RUN}/benchBenchmarkSeries-binary_header.out > ${DIR}/bench_outs/${RUN}/bench_new.out

sed 's/BenchmarkSeries\/index_json/BenchmarkSeries/g' ${DIR}/bench_outs/${RUN}/benchBenchmarkSeries-index_json.out > ${DIR}/bench_outs/${RUN}/bench_old.out

benchcmp ${DIR}/bench_outs/${RUN}/bench_old.out ${DIR}/bench_outs/${RUN}/bench_new.out | tee ${DIR}/bench_outs/${RUN}/bench.out
