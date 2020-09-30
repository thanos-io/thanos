import { RouteComponentProps } from "@reach/router";
import React, { FC } from "react";
import { useFetch } from "../../hooks/useFetch";
import PathPrefixProps from "../../types/PathPrefixProps";
import { Table } from 'reactstrap';
import { withStatusIndicator } from "../../components/withStatusIndicator";

interface ConfigFilesMap {
    [key: string]: string;
}

interface ConfigFileProps {
    data?: ConfigFilesMap
}

const ConfigFilesContent: FC<ConfigFileProps> = ({ data = {} }) => {
    return (
      <>
      <h2>Configuration Files</h2>
      <Table bordered striped>
        <tbody>
          {Object.keys(data).map(key => (
            <tr key={key}>
              <th>{key}</th>
              <td>{data[key]}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    </>
    )
} 
const ConfigFilesWithStatusIndicator = withStatusIndicator(ConfigFilesContent);

const ConfigFiles: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix = '' }) => {
    const { response, error, isLoading } = useFetch<ConfigFilesMap>(`${pathPrefix}/api/v1/status/configfiles`);
    return <ConfigFilesWithStatusIndicator data={response.data} error={error} isLoading={isLoading} />
}

export default ConfigFiles;