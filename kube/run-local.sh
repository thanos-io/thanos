#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${DIR}/envs.sh

usage() { echo "Usage: $0 [-d <vm-driver>] (specify vm-driver, by default none - works only on linux) [-i] (install required binaries)" 1>&2; exit 1; }

install() {
    mkdir -p bin
    pushd ${DIR}/bin
        echo "Downloading kubectl 1.9 locally"
        curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v1.9.0/bin/linux/amd64/kubectl && chmod +x kubectl

        echo "Downloading minikube"
        curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && chmod +x minikube
    popd
}

DRIVER="none"
while getopts ":d:i" o; do
    case "${o}" in
        d)
            DRIVER=${OPTARG}
            ;;
        i)
            install
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))

echo "Starting local k8s cluster with config inside ${KUBECONFIG}. To use it, you need to do
'source kube/envs.sh' to set up needed environment variables. You can stop the local k8s cluster using:
 minikube stop. Also make sure you have 'namespace 10.0.0.10' inside the /etc/resolv.conf to have kube-dns entries
 accessible on your host."

mkdir -p .kube || true
touch .kube/config

if minikube status | grep -E "(Stopped)|minikube: $"; then
    sudo -E ${DIR}/bin/minikube start --vm-driver=${DRIVER} --kubernetes-version=v1.9.0
fi

# This for loop waits until kubectl can access the api server that Minikube has created.
for i in {1..150}; do # timeout for 5 minutes
   kubectl get po &> /dev/null
   if [ $? -ne 1 ]; then
      break
  fi
  sleep 2
done

# Making sure in best-effort way that k8s generates fresh certs.
kubectl delete secret $(kubectl get secret | grep default-token | cut -d " " -f 1) 2>/dev/null || true
kubectl delete secret -n kube-public $(kubectl get secret -n kube-public | grep default-token | cut -d " " -f 1) 2>/dev/null || true
kubectl delete secret -n kube-system $(kubectl get secret -n kube-system | grep default-token | cut -d " " -f 1) 2>/dev/null || true

echo "Cluster is running. See README.md for example deployments you can apply."