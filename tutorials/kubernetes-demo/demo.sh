#!/usr/bin/env bash

########################
# include the magic
########################
. demo-magic.sh

clear

# We assume ./setup.sh was successfully ran.

pe "ls -l"

p "echo 'x'"

echo "lol"

# US, EU, ASIA
pe "kubectl --context=leaf1 -n=kube-system get po"

