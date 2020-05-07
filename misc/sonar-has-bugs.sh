#!/bin/bash

# returns (exit code): n_bug

# prerequisite:
# - curl, jq have been installed
# - $SONAR_HOST, $SONAR_PROJECT_KEY have been set

set -e
set -u

# -sS: do not show progress but show errors
N_BUG=$(curl -sS "$SONAR_HOST/api/issues/search?projectKey=$SONAR_PROJECT_KEY&facets=types&types=BUG&statuses=OPEN,CONFIRMED,REOPENED" | jq -r '.issues|length')

echo "sonar bugs: $N_BUG"

exit $N_BUG
