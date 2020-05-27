#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e -o pipefail

# Configurations
CXX="g++"

# Resolves directory paths
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( cd "`dirname $0`"/../..; pwd )"

# Resolves the current platform
OS="$( cd ${ROOT_DIR}/lib && java org.xerial.snappy.OSInfo --os )"
ARCH="$( cd ${ROOT_DIR}/lib && java org.xerial.snappy.OSInfo --arch )"

# Loads some variables from `pom.xml`
. ${ROOT_DIR}/bin/package.sh && get_package_variables_from_pom "${ROOT_DIR}"

# Downloads any application tarball given a URL, the expected tarball name,
# and, optionally, a checkable binary path to determine if the binary has
# already been installed
## Arg1 - URL
## Arg2 - Tarball Name
## Arg3 - Checkable Binary
download_app() {
  local remote_tarball="$1/$2"
  local local_tarball="${BASE_DIR}/$2"
  local binary="${BASE_DIR}/$3"

  # setup `curl` and `wget` silent options if we're running on Jenkins
  local curl_opts="-L"
  local wget_opts=""
  if [ -n "$AMPLAB_JENKINS" ]; then
    curl_opts="-s ${curl_opts}"
    wget_opts="--quiet ${wget_opts}"
  else
    curl_opts="--progress-bar ${curl_opts}"
    wget_opts="--progress=bar:force ${wget_opts}"
  fi

  if [ -z "$3" -o ! -f "$binary" ]; then
    # check if we already have the tarball
    # check if we have curl installed
    # download application
    [ ! -f "${local_tarball}" ] && [ $(command -v curl) ] && \
      echo "exec: curl ${curl_opts} ${remote_tarball}" 1>&2 && \
      curl ${curl_opts} "${remote_tarball}" > "${local_tarball}"
    # if the file still doesn't exist, lets try `wget` and cross our fingers
    [ ! -f "${local_tarball}" ] && [ $(command -v wget) ] && \
      echo "exec: wget ${wget_opts} ${remote_tarball}" 1>&2 && \
      wget ${wget_opts} -O "${local_tarball}" "${remote_tarball}"
    # if both were unsuccessful, exit
    [ ! -f "${local_tarball}" ] && \
      echo -n "ERROR: Cannot download $2 with cURL or wget; " && \
      echo "please download manually and try again." && \
      exit 2
    cd "${BASE_DIR}" && tar -xvf "$2"
    rm -rf "$local_tarball"
  fi
}

compile_sqlsmith_from_source() {
  # Fetches the SQLSmith source code with the version
  # https://github.com/anse1/sqlsmith/releases/download/v${SQLSMITH_VERSION}/sqlsmith-${SQLSMITH_VERSION}.tar.gz
  download_app \
    "https://github.com/anse1/sqlsmith/releases/download/v${SQLSMITH_VERSION}" \
    "sqlsmith-${SQLSMITH_VERSION}.tar.gz" \
    "sqlsmith-${SQLSMITH_VERSION}/configure"

  # Compiles the Numba runtime library w/o OpenMP
  local src="${BASE_DIR}/sqlsmith-${SQLSMITH_VERSION}"
  local binary="${src}/libsqlsmith-${SQLSMITH_VERSION}.dylib"

  # TODO: Removes this
  local pqxx="/usr/local/Cellar/libpqxx@6/6.4.6"

  # TODO: Makes the dependencies of the shared library less
  [ ! -f "${binary}" ] && cd ${src} && patch -p1 < ../sqlsmith-${SQLSMITH_VERSION}.patch && \
    LIBPQXX_CFLAGS="-I${pqxx}/include" LIBPQXX_LIBS="-L${pqxx}/lib -lpqxx" ./configure && make && \
    ${CXX} -shared -fPIC -std=c++11 -g -O2 \
      -I${src} -I${BASE_DIR} -I${BASE_DIR}/include -I${BASE_DIR}/include/${OS} \
      -I${pqxx}/include -L${pqxx}/lib \
      ${BASE_DIR}/shim.cc *.o \
      -o libsqlsmith-${SQLSMITH_VERSION}.dylib \
      -lpqxx -lpq -lsqlite3 & \
    cd ${BASE_DIR} &&
    # Wait until the commands finished
    wait

  # Checks if the compilation finished successfully
  [ ! -x "${binary}" ] && \
    echo -n "ERROR: SQLSmith compilation failed; please check failure reasons." && \
    exit 2

  SQLSMITH_LIB="${binary}"
}

# Builds `libsqlsmith-${SQLSMITH_VERSION}.dylib` from the SQLSmith codebase
compile_sqlsmith_from_source

# Copys the built lib into the package resource
DST=${ROOT_DIR}/src/main/resources/lib/${OS}/${ARCH}
echo "Copying libsqlsmith-${SQLSMITH_VERSION}.dylib to '${DST}'..."
cp ${SQLSMITH_LIB} ${DST}/libsqlsmith.dylib

