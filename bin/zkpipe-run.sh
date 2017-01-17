#!/usr/bin/env bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] <command> [opts]"
  exit 1
fi

# Exclude jars not necessary for running commands.
regex="(-(test|src|scaladoc|javadoc)\.jar|jar.asc)$"
should_include_file() {
  if [ "$INCLUDE_TEST_JARS" = true ]; then
    return 0
  fi
  file=$1
  if [ -z "$(echo "$file" | egrep "$regex")" ] ; then
    return 0
  else
    return 1
  fi
}

base_dir=$(dirname $0)/..

if [ -z "$SCALA_BINARY_VERSION" ]; then
  SCALA_BINARY_VERSION=2.12
fi

# JMX settings
if [ -z "ZKPIPE_JMX_OPTS" ]; then
  ZKPIPE_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  ZKPIPE_JMX_OPTS="$ZKPIPE_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
  LOG_DIR="$base_dir/logs"
fi

shopt -s nullglob

# classpath addition for release
for file in "$base_dir"/libs/*;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

for file in "$base_dir"/target/scala-${SCALA_BINARY_VERSION}/zkpipe-assembly-*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

shopt -u nullglob

# Generic jvm settings you want to add
if [ -z "$ZKPIPE_OPTS" ]; then
  ZKPIPE_OPTS=""
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$ZKPIPE_HEAP_OPTS" ]; then
  ZKPIPE_HEAP_OPTS="-Xmx256M"
fi

# JVM performance options
if [ -z "$ZKPIPE_JVM_PERFORMANCE_OPTS" ]; then
  ZKPIPE_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA $ZKPIPE_HEAP_OPTS $ZKPIPE_JVM_PERFORMANCE_OPTS $ZKPIPE_JMX_OPTS -cp $CLASSPATH $ZKPIPE_OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  exec $JAVA $ZKPIPE_HEAP_OPTS $ZKPIPE_JVM_PERFORMANCE_OPTS $ZKPIPE_JMX_OPTS -cp $CLASSPATH $ZKPIPE_OPTS "$@"
fi
