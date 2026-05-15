import React, { useState } from 'react';
import { Table, Tooltip } from 'reactstrap';
import { QueryTree, FanoutEntry } from './ListTree';

interface FanoutTableProps {
  node: QueryTree | null;
}

interface FanoutRow extends FanoutEntry {
  selector: string;
}

// extractMatchers turns a promql-engine selector String() like
// `[vectorSelector] {[__name__="up" aa="bb"]} 1 mod 11` (or the
// matrixSelector variants) into a standard PromQL selector
// `{__name__="up",aa="bb"}`.
const extractMatchers = (name: string): string | null => {
  const m = name.match(/\{\[([^\]]*)\]\}/);
  if (!m) {
    return null;
  }
  const parts = m[1].split(/\s+/).filter((s) => s.length > 0);
  return '{' + parts.join(',') + '}';
};

// findSelectorName walks down to the first descendant whose name carries
// matcher information so the fan-out row shows the actual selector rather
// than a wrapping coalesce/concurrent node.
const findSelectorName = (node: QueryTree): string => {
  const extracted = extractMatchers(node.name);
  if (extracted) {
    return extracted;
  }
  if (node.children) {
    for (const c of node.children) {
      const r = findSelectorName(c);
      if (r !== c.name) {
        return r;
      }
    }
  }
  return node.name;
};

const collectRows = (node: QueryTree, out: FanoutRow[]): void => {
  if (node.fanout && node.fanout.length > 0) {
    const selector = findSelectorName(node);
    for (const f of node.fanout) {
      out.push({ selector, ...f });
    }
  }
  if (node.children) {
    for (const c of node.children) {
      collectRows(c, out);
    }
  }
};

const FanoutTable: React.FC<FanoutTableProps> = ({ node }) => {
  const [titleTooltipOpen, setTitleTooltipOpen] = useState(false);
  const [chunksTooltipOpen, setChunksTooltipOpen] = useState(false);

  if (!node) {
    return null;
  }
  const rows: FanoutRow[] = [];
  collectRows(node, rows);
  if (rows.length === 0) {
    return null;
  }
  return (
    <>
      <h5 id="fanout-telemetry-title" style={{ display: 'inline-block', cursor: 'help' }}>
        Fan-out telemetry
      </h5>
      <Tooltip
        isOpen={titleTooltipOpen}
        toggle={() => setTitleTooltipOpen(!titleTooltipOpen)}
        target="fanout-telemetry-title"
        placement="right"
      >
        Fan-out telemetry measures what endpoints returned over the wire. It may differ from engine telemetry because the
        engine wraps results in iterators and only pulls the data it actually needs, so counters seen by the engine can be
        lower than what the endpoints sent.
      </Tooltip>
      <Table size="sm" bordered striped responsive>
        <thead>
          <tr>
            <th>Selector</th>
            <th>Address</th>
            <th>Duration</th>
            <th>Bytes</th>
            <th>Responses</th>
            <th>Series</th>
            <th id="fanout-chunks-header" style={{ cursor: 'help' }}>
              Chunks
            </th>
            <th>Samples</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              <td>{r.selector}</td>
              <td>{r.endpointAddr}</td>
              <td>{r.duration}</td>
              <td>{r.bytesProcessed ?? 0}</td>
              <td>{r.numResponses ?? 0}</td>
              <td>{r.series ?? 0}</td>
              <td>{r.chunks ?? 0}</td>
              <td>{r.samples ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </Table>
      <Tooltip
        isOpen={chunksTooltipOpen}
        toggle={() => setChunksTooltipOpen(!chunksTooltipOpen)}
        target="fanout-chunks-header"
        placement="left"
      >
        Chunks are the compressed blocks of contiguous samples that TSDB stores on disk. A single series is split into many
        chunks over time (typically ~120 samples per chunk), and endpoints return chunks rather than raw samples to keep the
        response small.
      </Tooltip>
    </>
  );
};

export default FanoutTable;
