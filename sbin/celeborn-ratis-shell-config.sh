#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
script="$(basename -- "${this}")"
this="${common_bin}/${script}"

# convert relative path to absolute path
config_bin=$(dirname "${this}")
script=$(basename "${this}")
config_bin=$(cd "${config_bin}"; pwd)
this="${config_bin}/${script}"

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  JAVA="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    JAVA="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

# Check Java version == 1.8 or == 11
JAVA_VERSION=$(${JAVA} -version 2>&1 | awk -F '"' '/version/ {print $2}')
JAVA_MAJORMINOR=$(echo "${JAVA_VERSION}" | awk -F. '{printf("%03d%03d",$1,$2);}')
JAVA_MAJOR=$(echo "${JAVA_VERSION}" | awk -F. '{printf("%03d",$1);}')
if [[ ${JAVA_MAJORMINOR} != 001008 && ${JAVA_MAJOR} != 011 ]]; then
  echo "Error: celeborn-ratis-shell requires Java 8 or Java 11, currently Java $JAVA_VERSION found."
  exit 1
fi

local CELEBORN_RATIS_SHELL_CLASSPATH

# load master jars since all ratis related in master
while read -d '' -r jarfile ; do
    if [[ "$CELEBORN_RATIS_SHELL_CLASSPATH" == "" ]]; then
        CELEBORN_RATIS_SHELL_CLASSPATH="$jarfile";
    else
        CELEBORN_RATIS_SHELL_CLASSPATH="$CELEBORN_RATIS_SHELL_CLASSPATH":"$jarfile"
    fi
done < <(find "$CELEBORN_HOME/master-jars" ! -type d -name '*.jar' -print0 | sort -z)

CELEBORN_RATIS_SHELL_CLIENT_CLASSPATH="${CELEBORN_CONF_DIR}/:${CELEBORN_RATIS_SHELL_CLASSPATH}"

CELEBORN_RATIS_SHELL_JAVA_OPTS+=" -Dratis.shell.logs.dir=${CELEBORN_LOG_DIR}"
CELEBORN_RATIS_SHELL_JAVA_OPTS+=" -Dlog4j.configuration=file:${CELEBORN_CONF_DIR}/ratis-log4j.properties"
CELEBORN_RATIS_SHELL_JAVA_OPTS+=" -Dorg.apache.jasper.compiler.disablejsr199=true"
CELEBORN_RATIS_SHELL_JAVA_OPTS+=" -Djava.net.preferIPv4Stack=true"
CELEBORN_RATIS_SHELL_JAVA_OPTS+=" -Dorg.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads=false"
