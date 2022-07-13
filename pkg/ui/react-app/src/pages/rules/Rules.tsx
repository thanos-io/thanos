import React, { FC } from 'react';
import PathPrefixProps from '../../types/PathPrefixProps';
import { useFetch } from '../../hooks/useFetch';
import { withStatusIndicator } from '../../components/withStatusIndicator';
import { RulesMap, RulesContent } from './RulesContent';

const RulesWithStatusIndicator = withStatusIndicator(RulesContent);

const Rules: FC<PathPrefixProps> = ({ pathPrefix }) => {
  const { response, error, isLoading } = useFetch<RulesMap>(`${pathPrefix}/api/v1/rules`);

  return <RulesWithStatusIndicator response={response} error={error} isLoading={isLoading} />;
};

export default Rules;
