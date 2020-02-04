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
  DropdownToggle,
  DropdownMenu,
  DropdownItem,
} from 'reactstrap';
import PathPrefixProps from '../types/PathPrefixProps';
import ThanosComponentProps from './types/ThanosComponentProps';

interface NavConfig {
  name: string;
  uri: string;
}

const navConfig: { [component: string]: NavConfig[] } = {
  query: [{ name: 'Graph', uri: '/new/graph' }],
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
            return (
              <NavItem key={config.uri}>
                <NavLink tag={Link} to={`${pathPrefix}${config.uri}`}>
                  {config.name}
                </NavLink>
              </NavItem>
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
