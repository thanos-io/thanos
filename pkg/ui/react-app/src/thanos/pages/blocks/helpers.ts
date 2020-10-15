import { LabelSet, Block, BlocksPool } from './block';

const stringify = (map: LabelSet): string => {
  let t = '';
  for (const [key, value] of Object.entries(map)) {
    t += `${key}: ${value} `;
  }
  return t;
};

const sortBlocksInRows = (blocks: Block[]): BlocksPool => {
  const pool: BlocksPool = {};

  blocks
    .sort((a, b) => a.thanos.downsample.resolution - b.thanos.downsample.resolution)
    .forEach(b => {
      if (!pool[`${b.compaction.level}-${b.thanos.downsample.resolution}`])
        pool[`${b.compaction.level}-${b.thanos.downsample.resolution}`] = [];

      pool[`${b.compaction.level}-${b.thanos.downsample.resolution}`].push(b);
    });

  return pool;
};

export const sortBlocks = (blocks: Block[], label: string): { [source: string]: BlocksPool } => {
  const titles: { [key: string]: string } = {};
  const pool: BlocksPool = {};

  blocks
    .sort((a, b) => a.compaction.level - b.compaction.level)
    .forEach(b => {
      const title = (function(): string {
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
  Object.keys(pool).forEach(k => {
    sortedPool[k] = sortBlocksInRows(pool[k]);
  });
  return sortedPool;
};

export const download = (blob: Block): string => {
  const url = window.URL.createObjectURL(new Blob([JSON.stringify(blob, null, 2)], { type: 'application/json' }));

  return url;
};
