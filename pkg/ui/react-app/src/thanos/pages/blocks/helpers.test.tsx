import { sortBlocks, isOverlapping } from './helpers';

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

const sorted = sortBlocks(overlapCaseData.blocks, overlapCaseData.label);
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
