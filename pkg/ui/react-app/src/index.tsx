import './globals';
import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';
import './themes/app.scss';
import './themes/light.scss';
import './themes/dark.scss';
import './fonts/codicon.ttf';
import { isPresent } from './utils';

// Declared/defined in public/index.html, value replaced by Prometheus when serving bundle.
declare const GLOBAL_PATH_PREFIX: string;
declare const THANOS_COMPONENT: string;

let prefix = GLOBAL_PATH_PREFIX;
if (GLOBAL_PATH_PREFIX === '{{ pathPrefix }}' || GLOBAL_PATH_PREFIX === '/' || !isPresent(GLOBAL_PATH_PREFIX)) {
  // Either we are running the app outside of Prometheus, so the placeholder value in
  // the index.html didn't get replaced, or we have a '/' prefix, which we also need to
  // normalize to '' to make concatenations work (prefixes like '/foo/bar/' already get
  // their trailing slash stripped by Prometheus).
  prefix = '';
}
let thanosComponent = THANOS_COMPONENT;
if (THANOS_COMPONENT === '' || THANOS_COMPONENT === '{{ .Component }}') {
  thanosComponent = 'query';
}

ReactDOM.render(<App pathPrefix={prefix} thanosComponent={thanosComponent} />, document.getElementById('root'));
