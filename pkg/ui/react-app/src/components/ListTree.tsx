import React, { useState } from 'react';
import { InputProps, Collapse, ListGroupItem, ListGroup, Tooltip } from 'reactstrap';
import { ExplainTree } from '../pages/graph/ExpressionInput';

export interface QueryTree {
  name: string;
  executionTime?: string;
  peakSamples?: number;
  totalSamples?: number;
  children?: QueryTree[];
}

interface NodeProps extends InputProps {
  node: QueryTree | ExplainTree | null;
}

const ListTree: React.FC<NodeProps> = ({ id, node }) => {
  type idMapping = {
    [key: string]: boolean;
  };

  const [mapping, setMapping] = useState<idMapping>({});
  const [peakSamplesTooltipOpen, setPeakSamplesTooltipOpen] = useState<idMapping>({});
  const [totalSamplesTooltipOpen, setTotalSamplesTooltipOpen] = useState<idMapping>({});
  const [executionTimeTooltipOpen, setExecutionTimeTooltipOpen] = useState<idMapping>({});
  const toggleTotalSamplesTooltip = (e: React.MouseEvent<HTMLDivElement>) => {
    const el = e.target as HTMLDivElement;
    const id = el.getAttribute('id');
    if (id) {
      setTotalSamplesTooltipOpen({ ...totalSamplesTooltipOpen, [id]: !totalSamplesTooltipOpen[id] });
    }
  };
  const toggleExecutionTimeTooltip = (e: React.MouseEvent<HTMLDivElement>) => {
    const el = e.target as HTMLDivElement;
    const id = el.getAttribute('id');
    if (id) {
      setExecutionTimeTooltipOpen({ ...executionTimeTooltipOpen, [id]: !executionTimeTooltipOpen[id] });
    }
  };
  const togglePeakSamplesTooltip = (e: React.MouseEvent<HTMLDivElement>) => {
    const el = e.target as HTMLDivElement;
    const id = el.getAttribute('id');
    if (id) {
      setPeakSamplesTooltipOpen({ ...peakSamplesTooltipOpen, [id]: !peakSamplesTooltipOpen[id] });
    }
  };
  const toggle = (e: React.MouseEvent<HTMLDivElement>) => {
    const el = e.target as HTMLDivElement;
    const id = el.getAttribute('id');
    if (id) {
      setMapping({ ...mapping, [id]: !mapping[id] });
    }
  };

  // Constructing List Items recursively.
  const mapper = (nodes: QueryTree[], parentId?: string, lvl?: number) => {
    return nodes.map((node: QueryTree, index: number) => {
      const id = `${index}-${parentId ? parentId : 'top'}`.replace(/[^a-zA-Z0-9-_]/g, '');
      const item = (
        <React.Fragment>
          <ListGroupItem
            className={`bg-transparent p-0 border-0 ${parentId ? `rounded-0 ${lvl ? 'border-bottom-0' : ''}` : ''}`}
          >
            {
              <div className={`d-flex align-items-center`} style={{ paddingLeft: `${25 * (lvl || 0)}px` }}>
                {node.children && (
                  <div className="pl-0 btn text-primary" style={{ cursor: 'inherit' }} color="link">
                    {mapping[id] ? '\u002B' : '\u002D'}
                  </div>
                )}
                <div id={id} style={{ cursor: `${node.children ? 'pointer' : 'inherit'}` }} onClick={toggle}>
                  {node.name}
                  {node.executionTime && (
                    <>
                      <span id={`executionTime-${id}`} style={{ paddingLeft: `20px` }}>
                        {node.executionTime}
                      </span>
                      <Tooltip
                        isOpen={executionTimeTooltipOpen[`executionTime-${id}`]}
                        toggle={toggleExecutionTimeTooltip}
                        target={`executionTime-${id}`}
                        placement="right"
                      >
                        Wall clock time
                      </Tooltip>
                    </>
                  )}
                  {node.peakSamples && (
                    <>
                      <span id={`peakSamples-${id}`} style={{ paddingLeft: `20px` }}>
                        {node.peakSamples}
                      </span>
                      <Tooltip
                        isOpen={peakSamplesTooltipOpen[`peakSamples-${id}`]}
                        toggle={togglePeakSamplesTooltip}
                        target={`peakSamples-${id}`}
                        placement="right"
                      >
                        Peak samples (in all steps)
                      </Tooltip>
                    </>
                  )}
                  {node.totalSamples && (
                    <>
                      <span id={`totalSamples-${id}`} style={{ paddingLeft: `20px` }}>
                        {node.totalSamples}
                      </span>
                      <Tooltip
                        isOpen={totalSamplesTooltipOpen[`totalSamples-${id}`]}
                        toggle={toggleTotalSamplesTooltip}
                        target={`totalSamples-${id}`}
                        placement="right"
                      >
                        Total samples (sum of all steps)
                      </Tooltip>
                    </>
                  )}
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
