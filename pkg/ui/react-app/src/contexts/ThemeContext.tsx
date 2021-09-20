/*
 *
 * THIS FILE WAS COPIED INTO THANOS FROM PROMETHEUS
 * (LIVING AT https://github.com/prometheus/prometheus/blob/main/web/ui/react-app/src/contexts/ThemeContext.tsx),
 * THE ORIGINAL CODE WAS LICENSED UNDER AN APACHE 2.0 LICENSE, SEE
 * https://github.com/prometheus/prometheus/blob/main/LICENSE.
 *
 */

import React from 'react';

export type themeName = 'light' | 'dark';
export type themeSetting = themeName | 'auto';

export interface ThemeCtx {
  theme: themeName;
  userPreference: themeSetting;
  setTheme: (t: themeSetting) => void;
}

// defaults, will be overridden in App.tsx
export const ThemeContext = React.createContext<ThemeCtx>({
  theme: 'light',
  userPreference: 'auto',
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  setTheme: (s: themeSetting) => {},
});

export const useTheme = () => {
  return React.useContext(ThemeContext);
};
