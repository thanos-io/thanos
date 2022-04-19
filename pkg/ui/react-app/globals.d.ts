import { Moment } from 'moment';

declare global {
  interface Window {
    jQuery: JQueryStatic;
    moment: Moment;
    THANOS_QUERY_URL: string;
  }
}
