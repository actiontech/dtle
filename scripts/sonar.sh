#!/bin/bash

# prerequisite:
# - sonar-scanner (and jvm) have been installed
# - $PWD is project root
# - $SONAR_HOST, $SONAR_PROJECT_KEY, $SONAR_TOKEN have been be set

set -e
set -u

if [ ! -f Gopkg.toml ]; then
    echo "This script must be run under project root."
    exit 1
fi

sonar-scanner \
  -Dsonar.projectKey=$SONAR_PROJECT_KEY \
  -Dsonar.sources="$PWD" \
  -Dsonar.host.url="$SONAR_HOST" \
  -Dsonar.login="$SONAR_TOKEN" \
  '-Dsonar.exclusions=vendor/**,dist/**,helper/**'
