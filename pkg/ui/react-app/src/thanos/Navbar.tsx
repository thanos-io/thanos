import React, { FC, useState } from 'react';
import { Link } from '@reach/router';
import {
  Collapse,
  Navbar,
  NavbarToggler,
  Nav,
  NavItem,
  NavLink,
  UncontrolledDropdown,
  DropdownItem,
  DropdownMenu,
  DropdownToggle,
} from 'reactstrap';
import PathPrefixProps from '../types/PathPrefixProps';

interface NavConfig {
  name: string;
  uri: string;
}

interface NavDropDown {
  name: string;
  children: NavConfig[];
}

const navConfig: { [component: string]: (NavConfig | NavDropDown)[] } = {
  query: [
    { name: 'Graph', uri: '/new/graph' },
    { name: 'Stores', uri: '/new/stores' },
    {
      name: 'Status',
      children: [
        { name: 'Runtime & Build Information', uri: '/new/status' },
        { name: 'Command-Line Flags', uri: '/new/flags' },
      ],
    },
  ],
  rule: [
    { name: 'Alerts', uri: '/new/alerts' },
    { name: 'Rules', uri: '/new/rules' },
  ],
  bucket: [{ name: 'Blocks', uri: '/new/blocks' }],
  compact: [
    { name: 'Global Blocks', uri: '/new/blocks' },
    { name: 'Loaded Blocks', uri: '/new/loaded' },
  ],
  store: [{ name: 'Loaded Blocks', uri: '/new/loaded' }],
};

const defaultClassicUIRoute: { [component: string]: string } = {
  query: '/graph',
  rule: '/alerts',
  bucket: '/',
  compact: '/loaded',
  store: '/loaded',
};

interface NavigationProps {
  thanosComponent: string;
  defaultRoute: string;
}

const Navigation: FC<PathPrefixProps & NavigationProps> = ({ pathPrefix, thanosComponent, defaultRoute }) => {
  const [isOpen, setIsOpen] = useState(false);
  const toggle = () => setIsOpen(!isOpen);
  return (
    <Navbar className="mb-3" dark color="dark" expand="md" fixed="top">
      <NavbarToggler onClick={toggle} />
      <Link className="navbar-brand" to={`${pathPrefix}/new${defaultRoute}`}>
        Thanos - {thanosComponent[0].toUpperCase()}
        {thanosComponent.substr(1, thanosComponent.length)}
      </Link>
      <Collapse isOpen={isOpen} navbar style={{ justifyContent: 'space-between' }}>
        <Nav className="ml-0" navbar>
          {navConfig[thanosComponent].map((config) => {
            if ('uri' in config)
              return (
                <NavItem key={config.uri}>
                  <NavLink tag={Link} to={`${pathPrefix}${config.uri}`}>
                    {config.name}
                  </NavLink>
                </NavItem>
              );

            return (
              <UncontrolledDropdown key={config.name} nav inNavbar>
                <DropdownToggle nav caret>
                  {config.name}
                </DropdownToggle>
                <DropdownMenu>
                  {config.children.map((c) => (
                    <DropdownItem key={c.uri} tag={Link} to={`${pathPrefix}${c.uri}`}>
                      {c.name}
                    </DropdownItem>
                  ))}
                </DropdownMenu>
              </UncontrolledDropdown>
            );
          })}
          <NavItem>
            <NavLink href="https://thanos.io/tip/thanos/getting-started.md/">Help</NavLink>
          </NavItem>
          <NavItem>
            <NavLink href={`${pathPrefix}${defaultClassicUIRoute[thanosComponent]}${window.location.search}`}>
              Classic UI
            </NavLink>
          </NavItem>
        </Nav>
      </Collapse>
    </Navbar>
  );
};

export default Navigation;
