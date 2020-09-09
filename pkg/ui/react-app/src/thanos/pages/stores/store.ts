export interface Label {
  name: string;
  value: string;
}

export interface Labels {
  labels: Label[];
}

export interface Store {
  name: string;
  minTime: number;
  maxTime: number;
  lastError: string | null;
  lastCheck: string;
  labelSets: Labels[];
}
