#!/usr/bin/env bash

kubectl --context=eu1 delete -f manifests
kubectl --context=us1 delete -f manifests