import * as React from 'react';
import { shallow } from 'enzyme';
import App from './App';
import Navigation from './thanos/Navbar';
import { Container } from 'reactstrap';
import { Alerts, Config, Flags, Rules, ServiceDiscovery, Status, Targets, TSDBStatus, PanelList } from './pages';
import { BrowserRouter as Router } from 'react-router-dom';

describe('App', () => {
  const app = shallow(<App pathPrefix="/path/prefix" thanosComponent="query" />);

  it('navigates', () => {
    expect(app.find(Navigation)).toHaveLength(1);
  });
  it('routes', () => {
    [Alerts, Config, Flags, Rules, ServiceDiscovery, Status, Targets, TSDBStatus, PanelList].forEach((component) => {
      const c = app.find(component);
      expect(c).toHaveLength(1);
      expect(c.prop('pathPrefix')).toBe('/path/prefix');
    });
    expect(app.find(Router)).toHaveLength(1);
    expect(app.find(Container)).toHaveLength(1);
  });
});
