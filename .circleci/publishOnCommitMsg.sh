#!/bin/bash

if git log -1 --pretty=%B | grep "^RELEASE.*";
then
   echo "Publishing a release on commit msg"
   ./gradlew distributeBuild
else
   echo "Not a release by commit msg"
fi

