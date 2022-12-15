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
import { ThemeToggle } from '../Theme';

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
    { name: 'Graph', uri: '/graph' },
    { name: 'Stores', uri: '/stores' },
    {
      name: 'Status',
      children: [
        { name: 'Runtime & Build Information', uri: '/status' },
        { name: 'Command-Line Flags', uri: '/flags' },
        { name: 'Alerts', uri: '/alerts' },
        { name: 'Targets', uri: '/targets' },
        { name: 'Rules', uri: '/rules' },
      ],
    },
  ],
  rule: [
    { name: 'Alerts', uri: '/alerts' },
    { name: 'Rules', uri: '/rules' },
    {
      name: 'Status',
      children: [
        { name: 'Runtime & Build Information', uri: '/status' },
        { name: 'Command-Line Flags', uri: '/flags' },
      ],
    },
  ],
  bucket: [
    { name: 'Blocks', uri: '/blocks' },
    {
      name: 'Status',
      children: [
        { name: 'Runtime & Build Information', uri: '/status' },
        { name: 'Command-Line Flags', uri: '/flags' },
      ],
    },
  ],
  compact: [
    { name: 'Global Blocks', uri: '/blocks' },
    { name: 'Loaded Blocks', uri: '/loaded' },
    {
      name: 'Status',
      children: [
        { name: 'Runtime & Build Information', uri: '/status' },
        { name: 'Command-Line Flags', uri: '/flags' },
      ],
    },
  ],
  store: [
    { name: 'Loaded Blocks', uri: '/loaded' },
    {
      name: 'Status',
      children: [
        { name: 'Runtime & Build Information', uri: '/status' },
        { name: 'Command-Line Flags', uri: '/flags' },
      ],
    },
  ],
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
      <NavbarToggler onClick={toggle} className="mr-2" />
      <Link className="navbar-brand" to={`${pathPrefix}${defaultRoute}`}>
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
        </Nav>
      </Collapse>
      <ThemeToggle />
    </Navbar>
  );
};

export default Navigation;
