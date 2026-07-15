#!/usr/bin/env bash
# Karpenter cluster management for CI.
# Sourced by run.sh — inherits set -euox pipefail and all global vars.

set -euox pipefail

KARPENTER_VERSION="1.13.0"
KARPENTER_CLUSTER_MAX_AGE_DAYS=3

function karpenter_create_cluster() {
  local cluster_name=$1
  local region=$2
  local kubeconfig=$3

  local karpenter_node_role="eksctl-KarpenterNodeRole-${cluster_name}"

  echo "=== Karpenter cluster: ${cluster_name} ==="

  # Check if cluster exists and is fresh enough
  if aws eks describe-cluster --name "${cluster_name}" --region "${region}" &>/dev/null; then
    local cluster_age_days
    cluster_age_days=$(karpenter_cluster_age_days "${cluster_name}" "${region}")
    if [ "${cluster_age_days}" -lt "${KARPENTER_CLUSTER_MAX_AGE_DAYS}" ]; then
      echo "Cluster exists and is ${cluster_age_days} days old, reusing."
      aws eks update-kubeconfig --name "${cluster_name}" --region "${region}" --kubeconfig="${kubeconfig}"
      # Ensure NodePool exists (idempotent)
      export CLUSTER_NAME="${cluster_name}"
      envsubst < "${BASE_DIR}/karpenter-nodepool.yaml" | $KUBECTL_BIN apply -f - --kubeconfig="${kubeconfig}"
      return 0
    fi
    echo "Cluster is ${cluster_age_days} days old, recreating..."
    karpenter_delete_cluster "${cluster_name}" "${region}" "${kubeconfig}"
  fi

  # Create cluster with eksctl built-in Karpenter
  echo "Creating cluster with Karpenter (~15 min)..."
  export CLUSTER_NAME="${cluster_name}"
  export AWS_REGION="${region}"
  envsubst < "${BASE_DIR}/karpenter-cluster.yaml" | $EKSCTL_BIN create cluster -f -
  aws eks update-kubeconfig --name "${cluster_name}" --region "${region}" --kubeconfig="${kubeconfig}"

  # Attach S3 policies to Karpenter node role (for Mountpoint credentials via IMDS)
  echo "Attaching S3 policies to ${karpenter_node_role}..."
  aws iam attach-role-policy \
    --role-name "${karpenter_node_role}" \
    --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess" || true
  aws iam put-role-policy \
    --role-name "${karpenter_node_role}" \
    --policy-name "S3ExpressAccess" \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3express:*"],"Resource":"*"}]}' || true

  # Apply NodePool + EC2NodeClass
  echo "Applying NodePool..."
  envsubst < "${BASE_DIR}/karpenter-nodepool.yaml" | $KUBECTL_BIN apply -f - --kubeconfig="${kubeconfig}"

  echo "Karpenter cluster ready."
}

function karpenter_delete_cluster() {
  local cluster_name=$1
  local region=$2
  local kubeconfig=$3

  echo "Deleting Karpenter cluster ${cluster_name}..."

  if aws eks describe-cluster --name "${cluster_name}" --region "${region}" &>/dev/null; then
    aws eks update-kubeconfig --name "${cluster_name}" --region "${region}" --kubeconfig="${kubeconfig}" 2>/dev/null || true
    # Delete NodePool so Karpenter terminates its nodes
    $KUBECTL_BIN delete nodepool default --ignore-not-found --timeout=10s --kubeconfig="${kubeconfig}" 2>/dev/null || true
    $KUBECTL_BIN delete ec2nodeclass default --ignore-not-found --timeout=10s --kubeconfig="${kubeconfig}" 2>/dev/null || true
    $KUBECTL_BIN wait --for=delete nodes -l karpenter.sh/nodepool --timeout=60s --kubeconfig="${kubeconfig}" 2>/dev/null || true

    # Delete cluster (handles VPC, node groups, Karpenter Helm, IAM)
    $EKSCTL_BIN delete cluster --name "${cluster_name}" --region "${region}" --disable-nodegroup-eviction
  fi
}

function karpenter_cluster_age_days() {
  local cluster_name=$1
  local region=$2
  local created_at
  created_at=$(aws eks describe-cluster --name "${cluster_name}" --region "${region}" \
    --query "cluster.createdAt" --output text)
  local created_epoch
  created_epoch=$(date -d "${created_at}" +%s)
  local now_epoch
  now_epoch=$(date +%s)
  echo $(( (now_epoch - created_epoch) / 86400 ))
}
