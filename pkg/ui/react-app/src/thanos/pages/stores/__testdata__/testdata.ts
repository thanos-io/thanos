import { StoreListProps } from '../Stores';

export const sampleAPIResponse: { status: string; data: StoreListProps } = {
  status: 'success',
  data: {
    sidecar: [
      {
        labelSets: [
          {
            monitor: 'prometheus_one',
          },
          {
            monitor: 'prometheus_two',
          },
        ],
        lastCheck: '2020-06-14T15:17:38.588378384Z',
        lastError: null,
        maxTime: 9223372036854776000,
        minTime: -62167219200000,
        guaranteedMinTime: -62167219200000,
        name: 'thanos_sidecar_one:10901',
      },
      {
        labelSets: [],
        lastCheck: '2020-06-14T15:17:38.588206741Z',
        lastError: 'some error message',
        maxTime: 92233720368547,
        minTime: 62167219200000,
        guaranteedMinTime: 62167219200000,
        name: 'thanos_sidecar_two:10901',
      },
    ],
    store: [
      {
        labelSets: [
          {
            monitor: 'prometheus_one',
          },
          {
            monitor: 'prometheus_one',
            source: 'Thanos',
          },
          {
            monitor: 'prometheus_one',
            source: 'Thanos1',
          },
          {
            monitor: 'prometheus_two',
          },
          {
            monitor: 'prometheus_two',
            source: 'Thanos',
          },
          {
            monitor: 'prometheus_two',
            source: 'Thanos2',
          },
        ],
        lastCheck: '2020-06-14T15:17:38.588246826Z',
        lastError: null,
        maxTime: 1592136000000,
        minTime: 1589461363260,
        guaranteedMinTime: 1589461363260,
        name: 'thanos_store:10901',
      },
    ],
  },
};
