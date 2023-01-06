#!/bin/bash
set -ex

# Open new Issue (or comment on existing) after build failure

if [[ -z "$GH_TOKEN" ]]
then
  echo "The env var GH_TOKEN is missing"
  echo "Please define it as a GitHub PAT with write permissions to Issues"
  exit 1
fi

theDate="$(date '+%A (%Y-%m-%d)')"
theMessage="Nightly build failed on $theDate in run [$GITHUB_RUN_ID]($GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID)"

existing=$(gh issue list \
  --label "nightly-failure" \
  --limit 1 \
  --jq '.[].number' \
  --json "number" \
  --state "open")

if [[ -z "$existing" ]]
then
  # open new issue
  gh issue create \
    --assignee Shelnutt2,ihnorton \
    --body "$theMessage" \
    --label "nightly-failure" \
    --title "Nightly build failed on $theDate"
else
  # comment on existing issue
  gh issue comment "$existing" \
    --body "$theMessage"
fi

echo "Success!"
