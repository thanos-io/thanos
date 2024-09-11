import { PayloadAction, createSlice } from "@reduxjs/toolkit";
import { useAppSelector } from "./hooks";
import { initializeFromLocalStorage } from "./initializeFromLocalStorage";

interface Settings {
  ready: boolean;
  lookbackDelta: string;
  component: string;
  queryURL: string;
  tenantHeader: string;
  defaultTenant: string;
  displayTenantBox: boolean;
  pathPrefix: string;
  useLocalTime: boolean;
  enableQueryHistory: boolean;
  enableAutocomplete: boolean;
  enableSyntaxHighlighting: boolean;
  enableLinter: boolean;
  showAnnotations: boolean;
}

// Declared/defined in public/index.html, value replaced by Thanos when serving bundle.
declare const GLOBAL_PATH_PREFIX: string;
declare const GLOBAL_READY: string;
declare const THANOS_COMPONENT: string;
declare const THANOS_QUERY_URL: string;
declare const THANOS_TENANT_HEADER: string;
declare const THANOS_DEFAULT_TENANT: string;
declare const THANOS_DISPLAY_TENANT_BOX: string;

export const localStorageKeyUseLocalTime = "settings.useLocalTime";
export const localStorageKeyEnableQueryHistory = "settings.enableQueryHistory";
export const localStorageKeyEnableAutocomplete = "settings.enableAutocomplete";
export const localStorageKeyEnableSyntaxHighlighting =
  "settings.enableSyntaxHighlighting";
export const localStorageKeyEnableLinter = "settings.enableLinter";
export const localStorageKeyShowAnnotations = "settings.showAnnotations";

export const initialState: Settings = {
  ready: GLOBAL_READY === "true",
  lookbackDelta: "1h",
  component: THANOS_COMPONENT,
  queryURL: THANOS_QUERY_URL,
  tenantHeader: THANOS_TENANT_HEADER,
  defaultTenant: THANOS_DEFAULT_TENANT,
  displayTenantBox: THANOS_DISPLAY_TENANT_BOX === "true",
  pathPrefix: GLOBAL_PATH_PREFIX === "" ? "" : GLOBAL_PATH_PREFIX,
  useLocalTime: initializeFromLocalStorage<boolean>(
    localStorageKeyUseLocalTime,
    false
  ),
  enableQueryHistory: initializeFromLocalStorage<boolean>(
    localStorageKeyEnableQueryHistory,
    false
  ),
  enableAutocomplete: initializeFromLocalStorage<boolean>(
    localStorageKeyEnableAutocomplete,
    true
  ),
  enableSyntaxHighlighting: initializeFromLocalStorage<boolean>(
    localStorageKeyEnableSyntaxHighlighting,
    true
  ),
  enableLinter: initializeFromLocalStorage<boolean>(
    localStorageKeyEnableLinter,
    true
  ),
  showAnnotations: initializeFromLocalStorage<boolean>(
    localStorageKeyShowAnnotations,
    true
  ),
};

export const settingsSlice = createSlice({
  name: "settings",
  initialState,
  reducers: {
    updateSettings: (state, { payload }: PayloadAction<Partial<Settings>>) => {
      Object.assign(state, payload);
    },
  },
});

export const { updateSettings } = settingsSlice.actions;

export const useSettings = () => {
  return useAppSelector((state) => state.settings);
};

export default settingsSlice.reducer;
