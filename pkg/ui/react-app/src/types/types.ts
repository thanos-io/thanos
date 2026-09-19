import { Alert, RuleState } from '../pages/alerts/AlertContents';

export interface Metric {
  [key: string]: string;
}

export interface Histogram {
  count: string;
  sum: string;
  buckets?: [number, string, string, string][];
}

export type SampleValue = [number, string];
export type SampleHistogram = [number, Histogram];

export interface QueryParams {
  startTime: number;
  endTime: number;
  resolution: number;
  traceID: string;
}

export type Rule = {
  alerts: Alert[];
  annotations: Record<string, string>;
  duration: number;
  keepFiringFor: number;
  evaluationTime: string;
  health: string;
  labels: Record<string, string>;
  lastError?: string;
  lastEvaluation: string;
  name: string;
  query: string;
  state: RuleState;
  type: string;
};
