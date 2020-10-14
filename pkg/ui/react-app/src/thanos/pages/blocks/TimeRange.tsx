import React, { FC, useMemo } from 'react';
import Slider from 'rc-slider';
import 'rc-slider/assets/index.css';
import moment from 'moment';
import styles from './blocks.module.css';

const Range = Slider.Range;

interface TimeRangeProps {
  viewMinTime: number;
  viewMaxTime: number;
  gridMinTime: number;
  gridMaxTime: number;
  onChange: (times: number[]) => void;
}

const NUM_MARKS = 10;

const TimeRange: FC<TimeRangeProps> = ({ viewMinTime, viewMaxTime, gridMinTime, gridMaxTime, onChange }) => {
  const marks = useMemo(() => {
    const step = Math.ceil((gridMaxTime - gridMinTime) / NUM_MARKS);

    const marks: { [num: string]: string } = {};
    for (let i = gridMinTime + step; i < gridMaxTime; i += step) {
      marks[i] = moment.unix(i / 1000).format('ll');
    }

    return marks;
  }, [gridMinTime, gridMaxTime]);

  return (
    <div className={styles.timeRangeDiv}>
      <Range
        allowCross={false}
        min={gridMinTime}
        max={gridMaxTime}
        marks={marks}
        defaultValue={[viewMinTime, viewMaxTime]}
        onChange={onChange}
      />
      <div className={styles.timeRange}>
        <span>{moment.unix(viewMinTime / 1000).format('ll')}</span>
        <span>{moment.unix(viewMaxTime / 1000).format('ll')}</span>
      </div>
    </div>
  );
};

export default TimeRange;
