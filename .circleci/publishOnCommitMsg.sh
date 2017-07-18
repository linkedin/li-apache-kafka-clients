#!/bin/bash

if git log -1 --pretty=%B | grep "^RELEASE.*";
then
   echo "Publishing a release"
   ./gradlew distributeBuild
else
   echo "Not a release"
fi

