export interface Label {
  name: string;
  value: string;
}

export interface Labels {
  labels: Label[];
}

export interface Store {
  name: string;
  min_time: number;
  max_time: number;
  last_error: string | null;
  last_check: string;
  label_sets: Labels[];
  store_type: {};
}
