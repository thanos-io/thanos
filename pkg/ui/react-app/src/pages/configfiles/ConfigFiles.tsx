import { RouteComponentProps } from "@reach/router";
import React, { FC } from "react";
import { useFetch } from "../../hooks/useFetch";
import PathPrefixProps from "../../types/PathPrefixProps";

interface ConfigFilesMap {
    [key: string]: string;
}

const ConfigFiles: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix = '' }) => {
    const { response, error, isLoading } = useFetch<ConfigFilesMap>(`${pathPrefix}/api/v1/status/configfiles`);
    return (
        <h1>This is the main Configuration Page</h1>
    );
}

export default ConfigFiles;