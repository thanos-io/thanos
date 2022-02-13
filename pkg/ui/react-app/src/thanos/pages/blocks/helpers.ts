import { LabelSet, Block, BlocksPool } from './block';
import { Fuzzy, FuzzyResult } from '@nexucis/fuzzy';

const stringify = (map: LabelSet): string => {
  let t = '';
  for (const [key, value] of Object.entries(map)) {
    t += `${key}: ${value} `;
  }
  return t;
};

export const isOverlapping = (a: Block, b: Block): boolean => {
  if (a?.minTime <= b?.minTime) return b?.minTime < a?.maxTime;
  else return a?.minTime < b?.maxTime;
};

const determineRow = (block: Block, rows: Block[][], startWithRow: number): number => {
  if (rows.length === 0) return 0;

  const len = rows[startWithRow]?.length || 0;
  if (len === 0) return startWithRow;

  if (isOverlapping(rows[startWithRow][len - 1], block)) {
    // Blocks are overlapping, try next row.
    return determineRow(block, rows, startWithRow + 1);
  }
  return startWithRow;
};

const splitOverlappingBlocks = (blocks: Block[]): Block[][] => {
  const rows: Block[][] = [[]];
  if (blocks.length === 0) return rows;

  blocks.forEach((b) => {
    const r = determineRow(b, rows, 0);
    if (!rows[r]) rows[r] = [];
    rows[r].push(b);
  });
  return rows;
};

const sortBlocksInRows = (blocks: Block[], findOverlappingBlocks: boolean): BlocksPool => {
  const poolWithOverlaps: { [key: string]: Block[] } = {};

  blocks
    .sort((a, b) => {
      if (a.compaction.level - b.compaction.level) {
        return a.compaction.level - b.compaction.level;
      }
      if (a.thanos.downsample.resolution - b.thanos.downsample.resolution) {
        return a.thanos.downsample.resolution - b.thanos.downsample.resolution;
      }
      return a.minTime - b.minTime;
    })
    .forEach((b) => {
      const key = `${b.compaction.level}-${b.thanos.downsample.resolution}`;
      if (!poolWithOverlaps[key]) poolWithOverlaps[key] = [];

      poolWithOverlaps[key].push(b);
    });

  const pool: BlocksPool = {};

  Object.entries(poolWithOverlaps).forEach(([key, blks]) => {
    if (findOverlappingBlocks) {
      let maxTime = 0;
      const filteredOverlap = blks.filter((value, index) => {
        const isOverlap = maxTime > value.minTime;
        if (value.maxTime > maxTime) {
          maxTime = value.maxTime;
        }
        return isOverlap || isOverlapping(blks[index], blks[index + 1]);
      });
      pool[key] = splitOverlappingBlocks(filteredOverlap);
    } else {
      pool[key] = splitOverlappingBlocks(blks);
    }
  });

  return pool;
};

export const sortBlocks = (
  blocks: Block[],
  label: string,
  findOverlappingBlocks: boolean
): { [source: string]: BlocksPool } => {
  const titles: { [key: string]: string } = {};
  const pool: { [key: string]: Block[] } = {};

  blocks
    .sort((a, b) => a.compaction.level - b.compaction.level)
    .forEach((b) => {
      const title = (function (): string {
        const key = label !== '' && b.thanos.labels[label];

        if (key) {
          return key;
        } else {
          let t = titles[stringify(b.thanos.labels)];
          if (t === undefined) {
            t = String(Object.keys(titles).length + 1);
            titles[stringify(b.thanos.labels)] = t;
          }
          return t;
        }
      })();

      pool[title] = pool[title] ? pool[title].concat([b]) : [b];
    });

  const sortedPool: { [source: string]: BlocksPool } = {};
  Object.keys(pool).forEach((k) => {
    sortedPool[k] = sortBlocksInRows(pool[k], findOverlappingBlocks);
  });
  return sortedPool;
};

export const download = (blob: Block): string => {
  const url = window.URL.createObjectURL(new Blob([JSON.stringify(blob, null, 2)], { type: 'application/json' }));

  return url;
};

export const getBlockByUlid = (blocks: Block[], ulid: string): Block[] => {
  if (ulid === '') {
    return blocks;
  }

  const ulidArray = blocks.map((block) => block.ulid);
  const fuz = new Fuzzy({ caseSensitive: true });

  const result: FuzzyResult[] = fuz.filter(ulid, ulidArray);

  const resultIndex = result.map((value) => value.index);

  const blockResult = blocks.filter((block, index) => resultIndex.includes(index));
  return blockResult;
};

export const getBlocksByCompactionLevel = (blocks: Block[], compactionLevel: number): Block[] => {
  if (compactionLevel === 0 || Number.isNaN(compactionLevel)) {
    return blocks;
  }

  const blockResult = blocks.filter((block) => block.compaction.level === compactionLevel);
  return blockResult;
};

export const getFilteredBlockPools = (
  blockPools: { [source: string]: BlocksPool },
  filteredBlocks: Block[]
): { [source: string]: BlocksPool } => {
  const newblockPools: { [source: string]: BlocksPool } = {};
  Object.keys(blockPools).map((key: string) => {
    const poolArrayIndex = blockPools[key];
    const poolArray = poolArrayIndex[Object.keys(poolArrayIndex)[0]];
    for (let i = 0; i < filteredBlocks.length; i++) {
      if (JSON.stringify(filteredBlocks[i].thanos.labels) === JSON.stringify(poolArray[0][0].thanos.labels)) {
        Object.assign(newblockPools, { [key]: blockPools[key] });
        break;
      }
    }
  });
  return newblockPools;
};
