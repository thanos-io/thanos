declare const THANOS_QUERY_URL: string;
declare const THANOS_TENANT_HEADER: string;
declare const THANOS_DEFAULT_TENANT: string;
declare const THANOS_DISPLAY_TENANT_BOX: string;

export let queryURL = THANOS_QUERY_URL;
if (queryURL === '' || queryURL === '{{ .queryURL }}') {
  queryURL = 'http://localhost:10902';
}

export let defaultTenant = THANOS_DEFAULT_TENANT;
if (defaultTenant === '' || defaultTenant === '{{ .defaultTenant }}') {
  defaultTenant = 'default-tenant';
}

export let tenantHeader = THANOS_TENANT_HEADER;
if (tenantHeader === '' || tenantHeader === '{{ .tenantHeader }}') {
  tenantHeader = 'thanos-tenant';
}

export let displayTenantBox = THANOS_DISPLAY_TENANT_BOX;
if (displayTenantBox === '' || displayTenantBox === '{{ .displayTenantBox }}') {
  displayTenantBox = 'none';
}
