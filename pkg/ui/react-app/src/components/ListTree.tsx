import React, { useState } from 'react';
import { InputProps, Collapse, ListGroupItem, ListGroup } from 'reactstrap';

export interface QueryTree {
  name: string;
  children?: QueryTree[];
}

interface NodeProps extends InputProps {
  node: QueryTree | null;
}

const ListTree: React.FC<NodeProps> = ({ id, node }) => {
  type idMapping = {
    [key: string]: boolean;
  };

  const [mapping, setMapping] = useState<idMapping>({});
  const toggle = (e: React.MouseEvent<HTMLDivElement>) => {
    const el = e.target as HTMLDivElement;
    const id = el.getAttribute('id');
    if (id) {
      setMapping({ ...mapping, [id]: !mapping[id] });
    }
  };

  // Constructing List Items recursively.
  const mapper = (nodes: QueryTree[], parentId?: any, lvl?: any) => {
    return nodes.map((node: QueryTree, index: number) => {
      const id = `${index}-${parentId ? parentId : 'top'}`.replace(/[^a-zA-Z0-9-_]/g, '');
      const item = (
        <React.Fragment>
          <ListGroupItem
            className={`bg-transparent p-0 border-0 ${parentId ? `rounded-0 ${lvl ? 'border-bottom-0' : ''}` : ''}`}
          >
            {
              <div className={`d-flex align-items-center`} style={{ paddingLeft: `${25 * lvl}px` }}>
                {node.children && (
                  <div className="pl-0 btn text-primary" style={{ cursor: 'inherit' }} color="link">
                    {mapping[id] ? '\u002B' : '\u002D'}
                  </div>
                )}
                <div id={id} style={{ cursor: `${node.children ? 'pointer' : 'inherit'}` }} onClick={toggle}>
                  {node.name}
                </div>
              </div>
            }
          </ListGroupItem>
          {node.children && <Collapse isOpen={mapping[id]}>{mapper(node.children, id, (lvl || 0) + 1)}</Collapse>}
        </React.Fragment>
      );

      return item;
    });
  };

  if (node) {
    return <ListGroup>{mapper([node], id)}</ListGroup>;
  } else {
    return null;
  }
};

export default ListTree;
