import React, { FC } from 'react';
import { Container } from 'reactstrap';
import { BrowserRouter as Router, Redirect, Switch, Route } from 'react-router-dom';
import useMedia from 'use-media';
import { Alerts, Config, Flags, Rules, ServiceDiscovery, Status, Targets, TSDBStatus, PanelList, NotFound } from './pages';
import PathPrefixProps from './types/PathPrefixProps';
import ThanosComponentProps from './thanos/types/ThanosComponentProps';
import Navigation from './thanos/Navbar';
import { Stores, ErrorBoundary, Blocks } from './thanos/pages';
import { ThemeContext, themeName, themeSetting } from './contexts/ThemeContext';
import { Theme, themeLocalStorageKey } from './Theme';
import { useLocalStorage } from './hooks/useLocalStorage';
import { QueryParamProvider } from 'use-query-params';

const defaultRouteConfig: { [component: string]: string } = {
  query: '/graph',
  rule: '/alerts',
  bucket: '/blocks',
  compact: '/loaded',
  store: '/loaded',
};

const App: FC<PathPrefixProps & ThanosComponentProps> = ({ pathPrefix, thanosComponent }) => {
  const [userTheme, setUserTheme] = useLocalStorage<themeSetting>(themeLocalStorageKey, 'auto');
  const browserHasThemes = useMedia('(prefers-color-scheme)');
  const browserWantsDarkTheme = useMedia('(prefers-color-scheme: dark)');

  let theme: themeName;
  if (userTheme !== 'auto') {
    theme = userTheme;
  } else {
    theme = browserHasThemes ? (browserWantsDarkTheme ? 'dark' : 'light') : 'light';
  }

  return (
    <ThemeContext.Provider
      value={{ theme: theme, userPreference: userTheme, setTheme: (t: themeSetting) => setUserTheme(t) }}
    >
      <Theme />
      <ErrorBoundary>
        <Router basename={pathPrefix}>
          <Navigation
            pathPrefix={pathPrefix}
            thanosComponent={thanosComponent}
            defaultRoute={defaultRouteConfig[thanosComponent]}
          />
          <QueryParamProvider ReactRouterRoute={Route}>
            <Container fluid style={{ paddingTop: 70 }}>
              <Switch>
                <Redirect from="/" to={`${pathPrefix}${defaultRouteConfig[thanosComponent]}`} />
                <Route path="/graph">
                  <PanelList pathPrefix={pathPrefix} />
                </Route>
                <Route path="/alerts">
                  <Alerts pathPrefix={pathPrefix} />
                </Route>
                <Route path="/config">
                  <Config pathPrefix={pathPrefix} />
                </Route>
                <Route path="/flags">
                  <Flags pathPrefix={pathPrefix} />
                </Route>
                <Route path="/rules">
                  <Rules pathPrefix={pathPrefix} />
                </Route>
                <Route path="/service-discovery">
                  <ServiceDiscovery pathPrefix={pathPrefix} />
                </Route>
                <Route path="/status">
                  <Status pathPrefix={pathPrefix} />
                </Route>
                <Route path="/tsdb-status">
                  <TSDBStatus pathPrefix={pathPrefix} />
                </Route>
                <Route path="/targets">
                  <Targets pathPrefix={pathPrefix} />
                </Route>
                <Route path="/stores">
                  <Stores pathPrefix={pathPrefix} />
                </Route>
                <Route path="/blocks">
                  <Blocks pathPrefix={pathPrefix} />
                </Route>
                <Route path="/loaded">
                  <Blocks pathPrefix={pathPrefix} view="loaded" />
                </Route>
                <NotFound pathPrefix={pathPrefix} default defaultRoute={defaultRouteConfig[thanosComponent]} />
              </Switch>
            </Container>
          </QueryParamProvider>
        </Router>
      </ErrorBoundary>
    </ThemeContext.Provider>
  );
};

export default App;
