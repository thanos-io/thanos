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
import ThanosComponentProps from './types/ThanosComponentProps';

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
    { name: 'Status', children: [{ name: 'Command-Line Flags', uri: '/new/flags' }] },
  ],
};

const Navigation: FC<PathPrefixProps & ThanosComponentProps> = ({ pathPrefix, thanosComponent }) => {
  const [isOpen, setIsOpen] = useState(false);
  const toggle = () => setIsOpen(!isOpen);
  return (
    <Navbar className="mb-3" dark color="dark" expand="md" fixed="top">
      <NavbarToggler onClick={toggle} />
      <Link className="navbar-brand" to={`${pathPrefix}/new/graph`}>
        Thanos - {thanosComponent[0].toUpperCase()}
        {thanosComponent.substr(1, thanosComponent.length)}
      </Link>
      <Collapse isOpen={isOpen} navbar style={{ justifyContent: 'space-between' }}>
        <Nav className="ml-0" navbar>
          {navConfig[thanosComponent].map(config => {
            if ('uri' in config)
              return (
                <NavItem key={config.uri}>
                  <NavLink tag={Link} to={`${pathPrefix}${config.uri}`}>
                    {config.name}
                  </NavLink>
                </NavItem>
              );

            return (
              <UncontrolledDropdown nav inNavbar>
                <DropdownToggle nav caret>
                  {config.name}
                </DropdownToggle>
                <DropdownMenu>
                  {config.children.map(c => (
                    <DropdownItem tag={Link} to={`${pathPrefix}${c.uri}`}>
                      {c.name}
                    </DropdownItem>
                  ))}
                </DropdownMenu>
              </UncontrolledDropdown>
            );
          })}
          <NavItem>
            <NavLink href="https://thanos.io/getting-started.md/">Help</NavLink>
          </NavItem>
          <NavItem>
            <NavLink href={`${pathPrefix}/graph${window.location.search}`}>Classic UI</NavLink>
          </NavItem>
        </Nav>
      </Collapse>
    </Navbar>
  );
};

export default Navigation;
