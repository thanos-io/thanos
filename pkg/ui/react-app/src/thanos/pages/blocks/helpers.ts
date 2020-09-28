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
    .sort((a, b) => a.meta.thanos.downsample.resolution - b.meta.thanos.downsample.resolution)
    .forEach(b => {
      if (!pool[`${b.meta.compaction.level}-${b.meta.thanos.downsample.resolution}`])
        pool[`${b.meta.compaction.level}-${b.meta.thanos.downsample.resolution}`] = [];

      pool[`${b.meta.compaction.level}-${b.meta.thanos.downsample.resolution}`].push(b);
    });

  return pool;
};

export const sortBlocks = (blocks: Block[], label: string): { [source: string]: BlocksPool } => {
  const titles: { [key: string]: string } = {};
  const pool: BlocksPool = {};

  blocks
    .sort((a, b) => a.meta.compaction.level - b.meta.compaction.level)
    .forEach(b => {
      const title = (function (): string {
        const key = label !== '' && b.meta.thanos.labels[label];

        if (key) {
          return key;
        } else {
          let t = titles[stringify(b.meta.thanos.labels)];
          if (t === undefined) {
            t = String(Object.keys(titles).length + 1);
            titles[stringify(b.meta.thanos.labels)] = t;
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
export const formatSize = (size: number): string => {
  if (size < 1024) {
    return `${size.toFixed(2)} B`;
  }
  size = size / 1024;
  if (size < 1024) {
    return `${size.toFixed(2)} KiB`;
  }

  size = size / 1024;
  if (size < 1024) {
    return `${size.toFixed(2)} MiB`;
  }

  size = size / 1024;
  return `${size.toFixed(2)} GiB`;
};