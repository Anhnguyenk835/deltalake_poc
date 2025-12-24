#!/bin/bash
# Auto-detect JAVA_HOME
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export PATH="${JAVA_HOME}/bin:${PATH}"

echo "Java Home: ${JAVA_HOME}"
echo "Java Version:"
java -version

# Execute the CMD from Dockerfile
exec "$@"
