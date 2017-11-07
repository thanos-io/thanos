#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export MINIKUBE_WANTUPDATENOTIFICATION=false
export MINIKUBE_WANTKUBECTLDOWNLOADMSG=false
export MINIKUBE_WANTREPORTERRORPROMPT=false
export MINIKUBE_HOME=${DIR}
export CHANGE_MINIKUBE_NONE_USER=true

export PATH=${DIR}/bin:${PATH}
export KUBECONFIG=${DIR}/.kube/config
