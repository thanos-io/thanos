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
