import React, { ChangeEvent, FC, useEffect, useState } from 'react';
import Filter, { Expanded } from './Filter';
import { useFetch } from '../../hooks/useFetch';
import { groupTargets, ScrapePool, ScrapePools, Target } from './target';
import PathPrefixProps from '../../types/PathPrefixProps';
import { withStatusIndicator } from '../../components/withStatusIndicator';
import { useLocalStorage } from '../../hooks/useLocalStorage';
import { ToggleMoreLess } from '../../components/ToggleMoreLess';
import { KVSearch } from '@nexucis/kvsearch';
import styles from './ScrapePoolPanel.module.css';
import { Col, Collapse, Row } from 'reactstrap';
import SearchBar from '../../components/SearchBar';
import { ScrapePoolContent } from './ScrapePoolContent';

interface ScrapePoolListProps {
  activeTargets: Target[];
}

const kvSearch = new KVSearch<Target>({
  shouldSort: true,
  indexedKeys: ['labels', 'scrapePool', ['labels', /.*/]],
});

interface PanelProps {
  scrapePool: string;
  targetGroup: ScrapePool;
  expanded: boolean;
  toggleExpanded: () => void;
}

export const ScrapePoolPanel: FC<PanelProps> = (props: PanelProps) => {
  const modifier = props.targetGroup.upCount < props.targetGroup.targets.length ? 'danger' : 'normal';
  const id = `pool-${props.scrapePool}`;
  const anchorProps = {
    href: `#${id}`,
    id,
  };
  return (
    <div>
      <ToggleMoreLess event={props.toggleExpanded} showMore={props.expanded}>
        <a className={styles[modifier]} {...anchorProps}>
          {`${props.scrapePool} (${props.targetGroup.upCount}/${props.targetGroup.targets.length} up)`}
        </a>
      </ToggleMoreLess>
      <Collapse isOpen={props.expanded}>
        <ScrapePoolContent targets={props.targetGroup.targets} />
      </Collapse>
    </div>
  );
};

export const ScrapePoolListContent: FC<ScrapePoolListProps> = ({ activeTargets }) => {
  const initialPoolList = groupTargets(activeTargets);
  const [poolList, setPoolList] = useState<ScrapePools>(initialPoolList);
  const [targetList, setTargetList] = useState(activeTargets);
  const [filter, setFilter] = useLocalStorage('targets-page-filter', { showHealthy: true, showUnhealthy: true });

  const initialExpanded: Expanded = Object.keys(initialPoolList).reduce(
    (acc: { [scrapePool: string]: boolean }, scrapePool: string) => ({
      ...acc,
      [scrapePool]: true,
    }),
    {}
  );
  const [expanded, setExpanded] = useLocalStorage('targets-page-expansion-state', initialExpanded);
  const { showHealthy, showUnhealthy } = filter;

  const handleSearchChange = (e: ChangeEvent<HTMLTextAreaElement | HTMLInputElement>) => {
    if (e.target.value !== '') {
      const result = kvSearch.filter(e.target.value.trim(), activeTargets);
      setTargetList(result.map((value) => value.original));
    } else {
      setTargetList(activeTargets);
    }
  };

  useEffect(() => {
    const list = targetList.filter((t) => showHealthy || t.health.toLowerCase() !== 'up');
    setPoolList(groupTargets(list));
  }, [showHealthy, targetList]);

  return (
    <>
      <Row className="align-items-center">
        <Col xs="12" sm="6" md="4">
          <Filter filter={filter} setFilter={setFilter} expanded={expanded} setExpanded={setExpanded} />
        </Col>
        <Col xs="12" sm="6" md="8">
          <SearchBar handleChange={handleSearchChange} placeholder="Filter by endpoint or labels" />
        </Col>
      </Row>
      {Object.keys(poolList)
        .filter((scrapePool) => {
          const targetGroup = poolList[scrapePool];
          const isHealthy = targetGroup.upCount === targetGroup.targets.length;
          return (isHealthy && showHealthy) || (!isHealthy && showUnhealthy);
        })
        .map<JSX.Element>((scrapePool) => (
          <ScrapePoolPanel
            key={scrapePool}
            scrapePool={scrapePool}
            targetGroup={poolList[scrapePool]}
            expanded={expanded[scrapePool]}
            toggleExpanded={(): void => setExpanded({ ...expanded, [scrapePool]: !expanded[scrapePool] })}
          />
        ))}
    </>
  );
};
ScrapePoolListContent.displayName = 'ScrapePoolListContent';

const ScrapePoolListWithStatusIndicator = withStatusIndicator(ScrapePoolListContent);

const ScrapePoolList: FC<PathPrefixProps> = ({ pathPrefix }) => {
  const { response, error, isLoading } = useFetch<ScrapePoolListProps>(`${pathPrefix}/api/v1/targets?state=active`);
  const { status: responseStatus } = response;
  const badResponse = responseStatus !== 'success' && responseStatus !== 'start fetching';
  return (
    <ScrapePoolListWithStatusIndicator
      {...response.data}
      error={badResponse ? new Error(responseStatus) : error}
      isLoading={isLoading}
      componentTitle="Targets information"
    />
  );
};

export default ScrapePoolList;
