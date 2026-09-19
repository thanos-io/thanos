import { Moment } from 'moment';

declare global {
  interface Window {
    jQuery: JQueryStatic;
    moment: Moment;
    THANOS_QUERY_URL: string;
    THANOS_DEFAULT_TENANT: string;
    THANOS_TENANT_HEADER: string;
    THANOS_DISPLAY_TENANT_BOX: string;
  }
}
