#!/usr/bin/env bash

echo "Starting deploy script"

JAR_CREATION_METHOD=$1
if [ "$JAR_CREATION_METHOD" = "" ]
  then
    echo "JAR creation method variable must be set"
    exit 1
fi

if [ $JAR_CREATION_METHOD != "assembly" ] && [ $JAR_CREATION_METHOD != "package" ]
  then
    echo "JAR creation method must be 'assembly' or 'package', was '$JAR_CREATION_METHOD'"
    exit 1
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ $CURRENT_BRANCH != "master" ]
  then
    echo "Must be on master branch"
    exit 1
fi

PROJECT_NAME="$(sbt -no-colors name | tail -1)"
PROJECT_NAME="${PROJECT_NAME#* }"
if [ "$PROJECT_NAME" = "" ]
  then
    echo "PROJECT_NAME variable cannot be empty"
    exit 1
fi

PROJECT_VERSION="$(sbt -no-colors version | tail -1)"
PROJECT_VERSION="${PROJECT_VERSION#* }"
if [ "$PROJECT_VERSION" = "" ]
  then
    echo "PROJECT_VERSION variable cannot be empty"
    exit 1
fi

SCALA_VERSION="$(sbt -no-colors scalaVersion | tail -1)"
SCALA_VERSION="${SCALA_VERSION#* }"
if [ "$SCALA_VERSION" = "" ]
  then
    echo "SCALA_VERSION variable cannot be empty"
    exit 1
fi

SCALA_BINARY_VERSION=${SCALA_VERSION%.*}
if [ "$SCALA_BINARY_VERSION" = "" ]
  then
    echo "SCALA_BINARY_VERSION variable cannot be empty"
    exit 1
fi

# the SPARK_VERSION_REGULAR code supports these definitions: val sparkVersion = "2.2.0"
SPARK_VERSION_REGULAR=$(cat build.sbt | grep "val sparkVersion = " | cut -f 4 -d " " | tr -d '"')
# the SPARK_VERSION_SPARK_PACKAGES code supports these definitions: sparkVersion := "2.3.0"
SPARK_VERSION_SPARK_PACKAGES=$(cat build.sbt | grep "sparkVersion := " | cut -f 3 -d " " | tr -d '"')
if [ "$SPARK_VERSION_REGULAR" = "" ]
  then
    SPARK_VERSION=$SPARK_VERSION_SPARK_PACKAGES
  else
    SPARK_VERSION=$SPARK_VERSION_REGULAR
fi
if [ "$SPARK_VERSION" = "" ]
  then
    echo "SPARK_VERSION variable cannot be empty"
    exit 1
fi

SPARK_PROJECT_VERSION=${SPARK_VERSION}_${PROJECT_VERSION}

LATEST_GIT_TAG="$(git describe --abbrev=0 --tags)"
if [ $LATEST_GIT_TAG = v$SPARK_PROJECT_VERSION ]
  then
    echo "The git tag $LATEST_GIT_TAG already exists.  The project version (currently $SPARK_PROJECT_VERSION) needs to be bumped in the build.sbt file."
    exit 1
fi

echo Project name $PROJECT_NAME
echo Project version $PROJECT_VERSION
echo Scala version $SCALA_VERSION
echo Scala binary version $SCALA_BINARY_VERSION
echo Spark version $SPARK_VERSION
echo SPARK_PROJECT_VERSION $SPARK_PROJECT_VERSION

echo "Create a git commit to bump the version"
git commit -am "Version bump to $SPARK_PROJECT_VERSION"

echo "Push the version bump commit to master"
git push origin master

echo "Create a tag"
git tag v$SPARK_PROJECT_VERSION
git push origin v$SPARK_PROJECT_VERSION

echo "Create a JAR file"
sbt $JAR_CREATION_METHOD

echo "Create a GitHub release"
JAR_PATH=target/scala-${SCALA_BINARY_VERSION}/${PROJECT_NAME}_${SCALA_BINARY_VERSION}-${SPARK_VERSION}_${PROJECT_VERSION}.jar
hub release create -a $JAR_PATH -m "Release v${SPARK_PROJECT_VERSION}" v${SPARK_PROJECT_VERSION}

