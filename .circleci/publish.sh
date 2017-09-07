#!/bin/bash

set -e

currentTag=`git describe --tags`
buildVersion=`grep -oP '^[\s]*version[\s]*=[\s]*\K([^\s]*)(?=([\s]*))' gradle.properties`

echo current tag is $currentTag, build version is $buildVersion

if [ "x$currentTag" != "x$buildVersion" ]; then
  echo "current tag version does not match project version"
  exit 1
fi

echo "Publishing a release"
./gradlew distributeBuild

