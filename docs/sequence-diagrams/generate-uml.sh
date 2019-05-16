#!/bin/bash

java -jar plantuml.jar sidecar.puml
java -jar plantuml.jar store.puml
java -jar plantuml.jar query.puml
java -jar plantuml.jar compact.puml
java -jar plantuml.jar rule.puml