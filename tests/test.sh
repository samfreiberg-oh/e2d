#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Constants
THIS_SCRIPT=$(basename $0)
PADDING=$(printf %-${#THIS_SCRIPT}s " ")
SHARED_VOLUME_PATH='/mnt'
E2D_DIR="${SHARED_VOLUME_PATH}/bin"
E2D_BIN="${E2D_DIR}/e2d"
CA_DIR="${SHARED_VOLUME_PATH}/ca"
CA_KEY="${CA_DIR}/ca.key"
CA_CRT="${CA_DIR}/ca.crt"
CLIENT_CRT='./client.crt'
CLIENT_KEY='./client.key'
PEER_CRT='./peer.crt'
PEER_KEY='./peer.key'
SERVER_CRT='./server.crt'
SERVER_KEY='./server.key'
SNAP_DIR="${SHARED_VOLUME_PATH}/snapshots/"
E2D_VAR_DIR='/var/lib/etcd'
E2D_DATA_DIR="${E2D_VAR_DIR}/data"
E2D_MEMBER0='e2d0'
E2D_MEMBER1='e2d1'
E2D_MEMBER2='e2d2'
CURRENT_USER="$(whoami)"
TEST_USERNAME='k8s'
ETCD_VERSION='v3.5.0'
ETCDCTL_BIN="/usr/local/bin/etcdctl"
OS="$(uname | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64)
    ARCH='amd64'
    ;;
  aarch64|arm64)
    ARCH='arm64'
    ;;
  *)
    echo 'Arch $(uname -m) nor supported'
    exit 1
    ;;
esac

function usage() {
    echo "Usage:"
    echo "${THIS_SCRIPT} -p <Container profile, e.g. builder, leader, member, tester, container>"
    echo
    echo "Configures containers inside docker-compose to test e2d"
    exit 1
}

function create_test_user() {
  echo "Creating user and group ${TEST_USERNAME}"
  groupadd -r -g 1000 ${TEST_USERNAME}
  useradd -r -g ${TEST_USERNAME} -u 1000 ${TEST_USERNAME}
}

function install_base_rpms() {
  echo 'installing amazon linux 2 base RPMs for testing e2d'
  yum install -y \
    file \
    iproute \
    iputils \
    yum-utils \
    vim \
    telnet \
    procps \
    tar \
    tree
}

function install_etcdctl() {
  local ETCD_TAR='etcd.tar.gz'
  curl -sL "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-${OS}-${ARCH}.tar.gz" -o "${ETCD_TAR}"
  tar -xvzf "${ETCD_TAR}"
  mv "etcd-${ETCD_VERSION}-${OS}-${ARCH}/etcdctl" "${ETCDCTL_BIN}"
  rm -rf "${ETCD_TAR}" "etcd-${ETCD_VERSION}-${OS}-${ARCH}/"
  chmod +x "${ETCDCTL_BIN}"
}

function create_var_etcd_dir() {
  mkdir -p "${E2D_VAR_DIR}"
  chown -R ${TEST_USERNAME}: "${E2D_VAR_DIR}"
}

function container_setup() {
  install_base_rpms
  create_test_user
  create_var_etcd_dir
  install_etcdctl
}

function is_e2d_ready() {
  until [[ -x "${E2D_BIN}" ]]; do
    echo "${E2D_BIN} is not executable by ${CURRENT_USER}, yet"
    sleep 2
  done
  echo "${E2D_BIN} is executable by ${CURRENT_USER}"
}

function create_etcd_ca() {
  is_e2d_ready
  mkdir -p "${CA_DIR}" "${SNAP_DIR}"
  "${E2D_BIN}" pki init --ca-key "${CA_KEY}" --ca-cert "${CA_CRT}"
  chown -R ${TEST_USERNAME}: "${SHARED_VOLUME_PATH}/"
}

function is_ca_ready() {
  until su ${TEST_USERNAME} -c "test -O ${CA_KEY}" && su ${TEST_USERNAME} -c "test -O ${CA_CRT}"; do
    echo "${CA_KEY} and/or ${CA_CRT} are not owned by ${TEST_USERNAME}, yet"
    echo "contents of ${SHARED_VOLUME_PATH} are"
    tree "${SHARED_VOLUME_PATH}"
    stat "${CA_KEY}" "${CA_CRT}" || true
    sleep 2
  done
  echo "${CA_KEY} and ${CA_CRT} are owned by ${TEST_USERNAME}"
}

function get_e2d_name() {
  echo "${E2D_NAME:-$(cat /etc/hostname)}"
}

function generate_certs() {
  is_ca_ready
  ${E2D_BIN} pki gencerts --hosts "$(get_e2d_name)" --ca-cert "${CA_CRT}" --ca-key "${CA_KEY}"
  chown ${TEST_USERNAME}: *.{key,crt}
  echo "certs were generated"
}

function get_bootstrap_addrs() {
  case "$(get_e2d_name)" in
    e2d0)
      echo "$(get_e2d_name):7980,${E2D_MEMBER1}:7980,${E2D_MEMBER2}:7980"
      ;;
    e2d1)
      echo "$(get_e2d_name):7980,${E2D_MEMBER0}:7980,${E2D_MEMBER2}:7980"
      ;;
    e2d2)
      echo "$(get_e2d_name):7980,${E2D_MEMBER0}:7980,${E2D_MEMBER1}:7980"
      ;;
  esac
}

function run_e2d() {
  if [[ "$(get_e2d_name)" == 'e2d0' ]]; then
    create_etcd_ca
  fi
  generate_certs
  su ${TEST_USERNAME} -c "${E2D_BIN} run -n 3 --snapshot-url file://${SNAP_DIR} \
    --name=$(get_e2d_name) \
    --data-dir=${E2D_DATA_DIR} \
    --ca-cert=${CA_CRT} \
    --ca-key=${CA_KEY} \
    --peer-cert=${PEER_CRT} \
    --peer-key=${PEER_KEY} \
    --server-cert=${SERVER_CRT} \
    --server-key=${SERVER_KEY} \
    --bootstrap-addrs $(get_bootstrap_addrs)"
}

function is_leader_up() {
  until curl --cert "${CLIENT_CRT}" --key "${CLIENT_KEY}" -m 1 -k "https://${E2D_MEMBER0}:2379"; do
    echo "${E2D_MEMBER0} is not up yet"
    sleep 5
  done
  echo "${E2D_MEMBER0} is up and ready"
}

function client_setup() {
  export ETCDCTL_CACERT="${CA_CRT}"
  export ETCDCTL_CERT="${CLIENT_CRT}"
  export ETCDCTL_KEY="${CLIENT_KEY}"
  export ETCDCTL_ENDPOINTS="https://${E2D_MEMBER0}:2379,https://${E2D_MEMBER1}:2379,https://${E2D_MEMBER2}:2379"
}

function tests() {
  generate_certs
  client_setup
  is_leader_up
  etcdctl member list -w table
  etcdctl endpoint health -w table
  etcdctl endpoint status -w table
  sleep infinity
}

while getopts ":p:" opt; do
  case ${opt} in
    p)
      PROFILE=${OPTARG} ;;
    \?)
      usage ;;
    :)
      usage ;;
  esac
done

if [[ -z ${PROFILE:-""} ]] ; then
  usage
fi

case "${PROFILE}" in
  container)
    container_setup
    ;;
  member)
    run_e2d
    ;;
  tester)
    tests
    ;;
  client_setup)
    client_setup
    ;;
  *)
    usage
    ;;
esac
