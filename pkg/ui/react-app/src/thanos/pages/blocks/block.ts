export interface Block {
  compaction: {
    level: number;
    sources: string[];
    parents?: {
      maxTime: number;
      minTime: number;
      ulid: string;
    }[];
  };
  maxTime: number;
  minTime: number;
  stats: {
    numChunks: number;
    numSamples: number;
    numSeries: number;
  };
  thanos: {
    downsample: {
      resolution: number;
    };
    labels: LabelSet;
    source: string;
    files?: {
      rel_path: string;
      size_bytes?: number;
    }[];
  };
  ulid: string;
  version: number;
}

export interface LabelSet {
  [labelName: string]: string;
}

export interface BlocksPool {
  [key: string]: Block[][];
}

export interface BlockSizeStats {
  chunkBytes: number;
  indexBytes: number;
  totalBytes: number;
}
