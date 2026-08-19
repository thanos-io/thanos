import React, { FC } from 'react';
import { metricToSeriesName } from '../../utils';

interface SeriesNameProps {
  labels: { [key: string]: string } | null;
  format: boolean;
}

const SeriesName: FC<SeriesNameProps> = ({ labels, format }) => {
  const renderFormatted = (): React.ReactElement => {
    const labelNodes: React.ReactElement[] = [];
    let first = true;
    for (const label in labels) {
      if (label === '__name__') {
        continue;
      }

      labelNodes.push(
        <span key={label}>
          {!first && ', '}
          <span className="legend-label-name">{label}</span>=<span className="legend-label-value">"{labels[label]}"</span>
        </span>
      );

      if (first) {
        first = false;
      }
    }

    if (labels === null) {
      return <>scalar</>;
    }

    return (
      <div>
        <span className="legend-metric-name">{labels.__name__ || ''}</span>
        <span className="legend-label-brace">{'{'}</span>
        {labelNodes}
        <span className="legend-label-brace">{'}'}</span>
      </div>
    );
  };

  if (format) {
    return renderFormatted();
  }
  // Return a simple text node. This is much faster to scroll through
  // for longer lists (hundreds of items).
  return <>{labels ? metricToSeriesName(labels) : null}</>;
};

export default SeriesName;
