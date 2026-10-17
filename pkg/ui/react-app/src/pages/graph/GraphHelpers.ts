import $ from 'jquery';

import { escapeHTML } from '../../utils';
import { GraphProps, GraphSeries } from './Graph';
import moment from 'moment-timezone';
import { colorPool } from './ColorPool';

export const formatValue = (y: number | null): string => {
  if (y === null) {
    return 'null';
  }
  const absY = Math.abs(y);

  if (absY >= 1e24) {
    return (y / 1e24).toFixed(2) + 'Y';
  } else if (absY >= 1e21) {
    return (y / 1e21).toFixed(2) + 'Z';
  } else if (absY >= 1e18) {
    return (y / 1e18).toFixed(2) + 'E';
  } else if (absY >= 1e15) {
    return (y / 1e15).toFixed(2) + 'P';
  } else if (absY >= 1e12) {
    return (y / 1e12).toFixed(2) + 'T';
  } else if (absY >= 1e9) {
    return (y / 1e9).toFixed(2) + 'G';
  } else if (absY >= 1e6) {
    return (y / 1e6).toFixed(2) + 'M';
  } else if (absY >= 1e3) {
    return (y / 1e3).toFixed(2) + 'k';
  } else if (absY >= 1) {
    return y.toFixed(2);
  } else if (absY === 0) {
    return y.toFixed(2);
  } else if (absY < 1e-23) {
    return (y / 1e-24).toFixed(2) + 'y';
  } else if (absY < 1e-20) {
    return (y / 1e-21).toFixed(2) + 'z';
  } else if (absY < 1e-17) {
    return (y / 1e-18).toFixed(2) + 'a';
  } else if (absY < 1e-14) {
    return (y / 1e-15).toFixed(2) + 'f';
  } else if (absY < 1e-11) {
    return (y / 1e-12).toFixed(2) + 'p';
  } else if (absY < 1e-8) {
    return (y / 1e-9).toFixed(2) + 'n';
  } else if (absY < 1e-5) {
    return (y / 1e-6).toFixed(2) + 'µ';
  } else if (absY < 1e-2) {
    return (y / 1e-3).toFixed(2) + 'm';
  } else if (absY <= 1) {
    return y.toFixed(2);
  }
  throw Error("couldn't format a value, this is a bug");
};

export const getHoverColor = (color: string, opacity: number, stacked: boolean): string => {
  const { r, g, b } = $.color.parse(color);
  if (!stacked) {
    return `rgba(${r}, ${g}, ${b}, ${opacity})`;
  }
  /*
    Unfortunately flot doesn't take into consideration
    the alpha value when adjusting the color on the stacked series.
    TODO: find better way to set the opacity.
  */
  const base = (1 - opacity) * 255;
  return `rgb(${Math.round(base + opacity * r)},${Math.round(base + opacity * g)},${Math.round(base + opacity * b)})`;
};

export const toHoverColor = (index: number, stacked: boolean) => (series: GraphSeries, i: number) => ({
  ...series,
  color: getHoverColor(series.color, i !== index ? 0.3 : 1, stacked),
});

export const getOptions = (stacked: boolean, useLocalTime: boolean): jquery.flot.plotOptions => {
  return {
    grid: {
      hoverable: true,
      clickable: true,
      autoHighlight: true,
      mouseActiveRadius: 100,
    },
    legend: {
      show: false,
    },
    xaxis: {
      mode: 'time',
      showTicks: true,
      showMinorTicks: true,
      timeBase: 'milliseconds',
      timezone: useLocalTime ? 'browser' : undefined,
    },
    yaxis: {
      tickFormatter: formatValue,
    },
    crosshair: {
      mode: 'xy',
      color: '#bbb',
    },
    tooltip: {
      show: true,
      cssClass: 'graph-tooltip',
      content: (_, xval, yval, { series }): string => {
        const { labels, color } = series;
        let dateTime = moment(xval);
        if (!useLocalTime) {
          dateTime = dateTime.utc();
        }
        return `
            <div class="date">${dateTime.format('YYYY-MM-DD HH:mm:ss Z')}</div>
            <div>
              <span class="detail-swatch" style="background-color: ${color}"></span>
              <span>${labels.__name__ || 'value'}: <strong>${yval}</strong></span>
            <div>
            <div class="labels mt-1">
              ${Object.keys(labels)
                .map((k) =>
                  k !== '__name__' ? `<div class="mb-1"><strong>${k}</strong>: ${escapeHTML(labels[k])}</div>` : ''
                )
                .join('')}
            </div>
          `;
      },
      defaultTheme: false,
      lines: true,
    },
    series: {
      stack: stacked,
      lines: {
        lineWidth: stacked ? 1 : 2,
        steps: false,
        fill: stacked,
      },
      shadowSize: 0,
    },
    selection: {
      mode: 'x',
    },
  };
};

export const normalizeData = ({ queryParams, data }: GraphProps): GraphSeries[] => {
  // use default values
  const { startTime = 0, endTime = 0, resolution = 0 } = queryParams || {};
  return data.result.map(({ values, histograms, metric }, index) => {
    // Insert nulls for all missing steps.
    const data = [];
    let valPos = 0;
    let histogramPos = 0;

    for (let t = startTime; t <= endTime; t += resolution) {
      // Allow for floating point inaccuracy.
      const currentValue = values && values[valPos];
      const currentHistogram = histograms && histograms[histogramPos];
      if (currentValue && values.length > valPos && currentValue[0] < t + resolution / 100) {
        data.push([currentValue[0] * 1000, parseValue(currentValue[1])]);
        valPos++;
      } else if (currentHistogram && histograms.length > histogramPos && currentHistogram[0] < t + resolution / 100) {
        data.push([currentHistogram[0] * 1000, parseValue(currentHistogram[1].sum)]);
        histogramPos++;
      } else {
        data.push([t * 1000, null]);
      }
    }

    return {
      labels: metric !== null ? metric : {},
      color: colorPool[index % colorPool.length],
      data,
      index,
    };
  });
};

export const parseValue = (value: string): null | number => {
  const val = parseFloat(value);
  // "+Inf", "-Inf", "+Inf" will be parsed into NaN by parseFloat(). They
  // can't be graphed, so show them as gaps (null).
  return isNaN(val) ? null : val;
};
