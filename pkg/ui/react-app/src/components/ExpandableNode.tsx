import React from 'react';
import TreeView, { flattenTree } from 'react-accessible-treeview';
import { InputProps } from 'reactstrap';
import styles from './ExpandableNode.module.css';

export interface NodeTree {
  name: string;
  children?: NodeTree[];
}

interface NodeProps extends InputProps {
  node: NodeTree | null;
}

const ControlledExpandedNode: React.FC<NodeProps> = ({ node }) => {
  if (!node) {
    node = { name: '' };
  } else {
    node = { name: '', children: [node] };
  }

  const data = flattenTree(node);

  return (
    <div>
      <div>
        <TreeView
          data={data}
          className={styles.basic}
          aria-label="Controlled expanded node tree"
          nodeRenderer={({ element, getNodeProps, level, handleExpand }) => {
            return (
              <div {...getNodeProps()} style={{ paddingLeft: 20 * (level - 1) }}>
                {element.name}
              </div>
            );
          }}
        />
      </div>
    </div>
  );
};

export default ControlledExpandedNode;
