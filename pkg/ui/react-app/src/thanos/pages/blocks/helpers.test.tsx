import { getBlockSizeStats, getFilteredBlockPools, humanizeBytes, isOverlapping, sortBlocks } from './helpers';
import { sizeBlock } from './__testdata__/testdata';

// Number of blocks in data: 8.
const overlapCaseData = {
  blocks: [
    {
      compaction: {
        level: 1,
        sources: ['01EWZCKPP4K0WYRTZC9RPRM5QK'],
      },
      minTime: 1608034200000,
      maxTime: 1608034500000,
      stats: {
        numSamples: 6634538,
        numSeries: 2334,
        numChunks: 51057,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          monitor: 'prometheus_one',
        },
        source: 'sidecar',
      },
      ulid: '01EWZCKPP4K0WYRTZC9RPRM5QK',
      version: 1,
    },

    {
      compaction: {
        level: 1,
        sources: ['01ESK5B1WQB6QEZQ4P0YCQXEC4'],
      },
      minTime: 1608034200000,
      maxTime: 1608034500000,
      stats: {
        numSamples: 6634538,
        numSeries: 2334,
        numChunks: 51057,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          monitor: 'prometheus_one',
        },
        source: 'sidecar',
      },
      ulid: '01ESK5B1WQB6QEZQ4P0YCQXEC4',
      version: 1,
    },
    {
      compaction: {
        level: 1,
        sources: ['01ET8F8C73GGXH279R6YMTWFHY'],
      },
      minTime: 1608034500000,
      maxTime: 1608034800000,
      stats: {
        numSamples: 6979750,
        numSeries: 2333,
        numChunks: 58325,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          monitor: 'prometheus_one',
        },
        source: 'sidecar',
      },
      ulid: '01ET8F8C73GGXH279R6YMTWFHY',
      version: 1,
    },
    {
      compaction: {
        level: 1,
        sources: ['01EWZCA2CFC5CPJE8CF9TXBW9H'],
      },
      minTime: 1608034500000,
      maxTime: 1608034800000,
      stats: {
        numSamples: 6979750,
        numSeries: 2333,
        numChunks: 58325,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          monitor: 'prometheus_one',
        },
        source: 'sidecar',
      },
      ulid: '01EWZCA2CFC5CPJE8CF9TXBW9H',
      version: 1,
    },
    {
      compaction: {
        level: 1,
        sources: ['01EXYEAS52VZW5G1FPV4NPH2D1'],
      },
      minTime: 1608034500000,
      maxTime: 1608034800000,
      stats: {
        numSamples: 6979750,
        numSeries: 2333,
        numChunks: 58325,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          monitor: 'prometheus_one',
        },
        source: 'sidecar',
      },
      ulid: '01EXYEAS52VZW5G1FPV4NPH2D1',
      version: 1,
    },
    {
      compaction: {
        level: 1,
        sources: ['01EWZCC9E998R19K8FKSTWP776'],
      },
      minTime: 1608034400000,
      maxTime: 1608034700000,
      stats: {
        numSamples: 6979750,
        numSeries: 2333,
        numChunks: 58325,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          monitor: 'prometheus_one',
        },
        source: 'sidecar',
      },
      ulid: '01EWZCC9E998R19K8FKSTWP776',
      version: 1,
    },
    {
      compaction: {
        level: 1,
        sources: ['01EXYE0YB9JYCT48B6673H4YNS'],
      },
      minTime: 1608034600000,
      maxTime: 1608034800000,
      stats: {
        numSamples: 6979750,
        numSeries: 2333,
        numChunks: 58325,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          monitor: 'prometheus_one',
        },
        source: 'sidecar',
      },
      ulid: '01EXYE0YB9JYCT48B6673H4YNS',
      version: 1,
    },
    {
      compaction: {
        level: 1,
        sources: ['01EEF8AGCHTPJ1MZ8KH0SEJZ4E'],
      },
      minTime: 1608034250000,
      maxTime: 1608034350000,
      stats: {
        numSamples: 6979750,
        numSeries: 2333,
        numChunks: 58325,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          monitor: 'prometheus_one',
        },
        source: 'sidecar',
      },
      ulid: '01EEF8AGCHTPJ1MZ8KH0SEJZ4E',
      version: 1,
    },
  ],
  label: 'monitor',
};

// Total number of blocks = 8
const blockPools = {
  '1': {
    '1-0': [
      [
        {
          ulid: '01FV7ZG6MBEM5X5H08RXV27AJK',
          minTime: 1644166200000,
          maxTime: 1644166500000,
          stats: {
            numSamples: 168320,
            numSeries: 2809,
            numChunks: 2809,
          },
          compaction: {
            level: 1,
            sources: ['01FV7ZG6MBEM5X5H08RXV27AJK'],
          },
          version: 1,
          thanos: {
            labels: {
              prometheus: 'prom-1',
            },
            downsample: {
              resolution: 0,
            },
            source: 'sidecar',
            segment_files: ['000001'],
            files: [
              {
                rel_path: 'chunks/000001',
                size_bytes: 145198,
              },
              {
                rel_path: 'index',
                size_bytes: 252717,
              },
              {
                rel_path: 'meta.json',
              },
            ],
          },
        },
        {
          ulid: '01FV7ZSBKB4NN14FXD6WZ17EZP',
          minTime: 1644166500000,
          maxTime: 1644166800000,
          stats: {
            numSamples: 168320,
            numSeries: 2809,
            numChunks: 2809,
          },
          compaction: {
            level: 1,
            sources: ['01FV7ZSBKB4NN14FXD6WZ17EZP'],
          },
          version: 1,
          thanos: {
            labels: {
              prometheus: 'prom-1',
            },
            downsample: {
              resolution: 0,
            },
            source: 'sidecar',
            segment_files: ['000001'],
            files: [
              {
                rel_path: 'chunks/000001',
                size_bytes: 142780,
              },
              {
                rel_path: 'index',
                size_bytes: 252717,
              },
              {
                rel_path: 'meta.json',
              },
            ],
          },
        },
      ],
    ],
  },
  '2': {
    '1-0': [
      [
        {
          ulid: '01FV7B65Z2KR15ZKC3E9HCCNXH',
          minTime: 1644144900000,
          maxTime: 1644145200000,
          stats: {
            numSamples: 171320,
            numSeries: 2859,
            numChunks: 2859,
          },
          compaction: {
            level: 1,
            sources: ['01FV7B65Z2KR15ZKC3E9HCCNXH'],
          },
          version: 1,
          thanos: {
            labels: {
              prometheus: 'prom-2',
            },
            downsample: {
              resolution: 0,
            },
            source: 'sidecar',
            segment_files: ['000001'],
            files: [
              {
                rel_path: 'chunks/000001',
                size_bytes: 152262,
              },
              {
                rel_path: 'index',
                size_bytes: 257544,
              },
              {
                rel_path: 'meta.json',
              },
            ],
          },
        },
        {
          ulid: '01FV7BFAY2ZFJ0PK3872CPZHY8',
          minTime: 1644145200000,
          maxTime: 1644145500000,
          stats: {
            numSamples: 171320,
            numSeries: 2859,
            numChunks: 2859,
          },
          compaction: {
            level: 1,
            sources: ['01FV7BFAY2ZFJ0PK3872CPZHY8'],
          },
          version: 1,
          thanos: {
            labels: {
              prometheus: 'prom-2',
            },
            downsample: {
              resolution: 0,
            },
            source: 'sidecar',
            segment_files: ['000001'],
            files: [
              {
                rel_path: 'chunks/000001',
                size_bytes: 164725,
              },
              {
                rel_path: 'index',
                size_bytes: 257544,
              },
              {
                rel_path: 'meta.json',
              },
            ],
          },
        },
      ],
    ],
  },
  '3': {
    '1-0': [
      [
        {
          ulid: '01FT8X9MJF5G7PFRNGZBYT8SCS',
          minTime: 1643123700000,
          maxTime: 1643124000000,
          stats: {
            numSamples: 171320,
            numSeries: 2859,
            numChunks: 2859,
          },
          compaction: {
            level: 1,
            sources: ['01FT8X9MJF5G7PFRNGZBYT8SCS'],
          },
          version: 1,
          thanos: {
            labels: {
              prometheus: 'prom-2 random:2',
            },
            downsample: {
              resolution: 0,
            },
            source: 'sidecar',
            segment_files: ['000001'],
            files: [
              {
                rel_path: 'chunks/000001',
                size_bytes: 143670,
              },
              {
                rel_path: 'index',
                size_bytes: 257574,
              },
              {
                rel_path: 'meta.json',
              },
            ],
          },
        },
        {
          ulid: '01FT8XJSHDYNVJ0SWP2SGMC2DR',
          minTime: 1643124000000,
          maxTime: 1643124300000,
          stats: {
            numSamples: 171320,
            numSeries: 2859,
            numChunks: 2859,
          },
          compaction: {
            level: 1,
            sources: ['01FT8XJSHDYNVJ0SWP2SGMC2DR'],
          },
          version: 1,
          thanos: {
            labels: {
              prometheus: 'prom-2 random:2',
            },
            downsample: {
              resolution: 0,
            },
            source: 'sidecar',
            segment_files: ['000001'],
            files: [
              {
                rel_path: 'chunks/000001',
                size_bytes: 148750,
              },
              {
                rel_path: 'index',
                size_bytes: 257574,
              },
              {
                rel_path: 'meta.json',
              },
            ],
          },
        },
      ],
    ],
  },
  '4': {
    '1-0': [
      [
        {
          ulid: '01FT8XJRPTQ9VP1K1Y3M3RHK4R',
          minTime: 1643124000000,
          maxTime: 1643124300000,
          stats: {
            numSamples: 171320,
            numSeries: 2859,
            numChunks: 2859,
          },
          compaction: {
            level: 1,
            sources: ['01FT8XJRPTQ9VP1K1Y3M3RHK4R'],
          },
          version: 1,
          thanos: {
            labels: {
              prometheus: 'prom-1 random:1',
            },
            downsample: {
              resolution: 0,
            },
            source: 'sidecar',
            segment_files: ['000001'],
            files: [
              {
                rel_path: 'chunks/000001',
                size_bytes: 210856,
              },
              {
                rel_path: 'index',
                size_bytes: 257590,
              },
              {
                rel_path: 'meta.json',
              },
            ],
          },
        },
        {
          ulid: '01FT8XVXNNJCT16QQTFYDKRG7W',
          minTime: 1643124300000,
          maxTime: 1643124600000,
          stats: {
            numSamples: 171320,
            numSeries: 2859,
            numChunks: 2859,
          },
          compaction: {
            level: 1,
            sources: ['01FT8XVXNNJCT16QQTFYDKRG7W'],
          },
          version: 1,
          thanos: {
            labels: {
              prometheus: 'prom-1 random:1',
            },
            downsample: {
              resolution: 0,
            },
            source: 'sidecar',
            segment_files: ['000001'],
            files: [
              {
                rel_path: 'chunks/000001',
                size_bytes: 224409,
              },
              {
                rel_path: 'index',
                size_bytes: 257590,
              },
              {
                rel_path: 'meta.json',
              },
            ],
          },
        },
      ],
    ],
  },
  '5': {
    '1-0': [],
  },
};

// Total filtered blocks = 1
const filteredBlocks = [
  {
    ulid: '01FV7B65Z2KR15ZKC3E9HCCNXH',
    minTime: 1644144900000,
    maxTime: 1644145200000,
    stats: {
      numSamples: 171320,
      numSeries: 2859,
      numChunks: 2859,
    },
    compaction: {
      level: 1,
      sources: ['01FV7B65Z2KR15ZKC3E9HCCNXH'],
    },
    version: 1,
    thanos: {
      labels: {
        prometheus: 'prom-2',
      },
      downsample: {
        resolution: 0,
      },
      source: 'sidecar',
      segment_files: ['000001'],
      files: [
        {
          rel_path: 'chunks/000001',
          size_bytes: 152262,
        },
        {
          rel_path: 'index',
          size_bytes: 257544,
        },
        {
          rel_path: 'meta.json',
        },
      ],
    },
  },
];

const sorted = sortBlocks(overlapCaseData.blocks, overlapCaseData.label, true);
const filteredBlockPools = getFilteredBlockPools(blockPools, filteredBlocks);
const source = 'prometheus_one';

describe('overlapping blocks', () => {
  it('has 1 source', () => {
    expect(Object.keys(sorted)).toHaveLength(1);
  });

  it('has 1 level-resolution', () => {
    expect(Object.keys(sorted[source])).toHaveLength(1);
  });

  const rows = Object.values(sorted[source])[0];
  it('has 5 rows', () => {
    expect(rows).toHaveLength(5);
  });

  it('renders 2 blocks in first row', () => {
    expect(rows[0]).toHaveLength(2);
  });

  it('renders 2 blocks in second row', () => {
    expect(rows[1]).toHaveLength(2);
  });

  it('renders 2 blocks in third row', () => {
    expect(rows[2]).toHaveLength(2);
  });

  it('renders 1 block in fourth row', () => {
    expect(rows[3]).toHaveLength(1);
  });

  it('renders 1 block in fifth row', () => {
    expect(rows[4]).toHaveLength(1);
  });
});

describe('isOverlapping helper', () => {
  const b = overlapCaseData.blocks[0];
  it('should return true for perfectly overlapping blocks', () => {
    expect(isOverlapping({ ...b, minTime: 10, maxTime: 20 }, { ...b, minTime: 10, maxTime: 20 })).toBe(true);
  });

  it('should return true for partially overlapping blocks', () => {
    expect(isOverlapping({ ...b, minTime: 10, maxTime: 20 }, { ...b, minTime: 15, maxTime: 25 })).toBe(true);
  });

  it('should return false for non-overlapping blocks', () => {
    expect(isOverlapping({ ...b, minTime: 10, maxTime: 20 }, { ...b, minTime: 30, maxTime: 40 })).toBe(false);
  });

  it('should return false if second block starts where first ends (a.maxTime == b.minTime)', () => {
    expect(isOverlapping({ ...b, minTime: 10, maxTime: 20 }, { ...b, minTime: 20, maxTime: 30 })).toBe(false);
  });
});

describe('Block Pools', () => {
  it('should have exactly 5 objects', () => {
    expect(Object.keys(blockPools)).toHaveLength(5);
  });
});

describe('Filtered block pools', () => {
  const objectKeyArray = Object.keys(filteredBlockPools);
  const filteredBlockPoolArray =
    filteredBlockPools[objectKeyArray[0]][Object.keys(filteredBlockPools[objectKeyArray[0]])[0]][0];

  it('should have exactly one object', () => {
    expect(objectKeyArray).toHaveLength(1);
  });
  it('should have key equals 2', () => {
    expect(objectKeyArray[0]).toEqual('2');
  });
  it('should contain contain blocks having same labels', () => {
    expect(filteredBlockPoolArray[0].thanos.labels).toEqual(filteredBlockPoolArray[1].thanos.labels);
  });
  it('should contain the first block having exactly the same labels as in filteredBlocks', () => {
    expect(filteredBlockPoolArray[0].thanos.labels).toEqual(filteredBlocks[0].thanos.labels);
  });
});

describe('Block Size Stats', () => {
  it('should return the index size', () => {
    expect(getBlockSizeStats(sizeBlock)?.indexBytes).toEqual(257574);
  });
  it('should return the summed chunk size', () => {
    expect(getBlockSizeStats(sizeBlock)?.chunkBytes).toEqual(537014552);
  });
  it('should return total summed size', () => {
    expect(getBlockSizeStats(sizeBlock)?.totalBytes).toEqual(537272126);
  });
});

describe('sortBlocks with empty label', () => {
  const blocksWithLabels = [
    {
      compaction: {
        level: 1,
        sources: ['01EWZCKPP4K0WYRTZC9RPRM5QK'],
      },
      minTime: 1608034200000,
      maxTime: 1608034500000,
      stats: {
        numSamples: 6634538,
        numSeries: 2334,
        numChunks: 51057,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          cluster: 'prod-us-east',
          env: 'production',
        },
        source: 'sidecar',
      },
      ulid: '01EWZCKPP4K0WYRTZC9RPRM5QK',
      version: 1,
    },
    {
      compaction: {
        level: 1,
        sources: ['01ESK5B1WQB6QEZQ4P0YCQXEC4'],
      },
      minTime: 1608034200000,
      maxTime: 1608034500000,
      stats: {
        numSamples: 6634538,
        numSeries: 2334,
        numChunks: 51057,
      },
      thanos: {
        downsample: {
          resolution: 0,
        },
        labels: {
          cluster: 'prod-eu-west',
          env: 'production',
        },
        source: 'sidecar',
      },
      ulid: '01ESK5B1WQB6QEZQ4P0YCQXEC4',
      version: 1,
    },
  ];

  it('should use stringified labels as source keys when no label is specified', () => {
    const sorted = sortBlocks(blocksWithLabels, '', false);
    const sourceKeys = Object.keys(sorted);
    // When no label is specified, the full labels should be used as keys
    expect(sourceKeys).toHaveLength(2);
    // Keys should contain actual label information, not just numbers
    expect(sourceKeys.every((key) => key.includes('cluster:'))).toBe(true);
    expect(sourceKeys.every((key) => key.includes('env:'))).toBe(true);
  });

  it('should group blocks with same labels together', () => {
    const blocksWithSameLabels = [
      { ...blocksWithLabels[0], ulid: '01FIRST' },
      { ...blocksWithLabels[0], ulid: '02SECOND' },
    ];
    const sorted = sortBlocks(blocksWithSameLabels, '', false);
    // Both blocks have same labels, should be grouped under same key
    expect(Object.keys(sorted)).toHaveLength(1);
  });
});

describe('humanizeBytes', () => {
  it('should return Unknown for undefined', () => {
    expect(humanizeBytes(undefined)).toEqual('Unknown Bytes');
  });
  it('should return Unknown for NaN', () => {
    expect(humanizeBytes(Number.NaN)).toEqual('Unknown Bytes');
  });
  it('should return 0 Bytes', () => {
    expect(humanizeBytes(0)).toEqual('0 Byte');
  });
  it('should return Bytes', () => {
    expect(humanizeBytes(512)).toEqual('512.00 Bytes');
  });
  it('should return Kibibytes', () => {
    expect(humanizeBytes(Math.pow(2, 10) * 1.5)).toEqual('1.50 KiB');
  });
  it('should return Mebibytes', () => {
    expect(humanizeBytes(Math.pow(2, 20) * 1.5)).toEqual('1.50 MiB');
  });
  it('should return Gibibytes', () => {
    expect(humanizeBytes(Math.pow(2, 30) * 1.5)).toEqual('1.50 GiB');
  });
  it('should return Tebibytes', () => {
    expect(humanizeBytes(Math.pow(2, 40) * 1.5)).toEqual('1.50 TiB');
  });
  it('should return Pebibytes', () => {
    expect(humanizeBytes(Math.pow(2, 50) * 1.5)).toEqual('1.50 PiB');
  });
  it('should still return Pebibytes for bigger units', () => {
    expect(humanizeBytes(Math.pow(2, 60) * 2)).toEqual('2048.00 PiB');
  });
});
