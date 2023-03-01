export type Labels = Record<string, string>;

export interface Store {
  name: string;
  minTime: number;
  maxTime: number;
  guaranteedMinTime: number;
  lastError: string | null;
  lastCheck: string;
  labelSets: Labels[];
}
