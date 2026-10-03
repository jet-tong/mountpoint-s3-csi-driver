### Local cache on an NVMe instance store

Dev only. While deployed, the driver runs only on the NVMe node, so S3 volumes on other nodes do not mount. The node costs money while it exists.

```bash
# Set up
eksctl create nodegroup -f dev/mp-dev-nvme-nodegroup.yaml
kubectl apply -f dev/mp-dev-nvme-local-provisioner.yaml
helm upgrade --install aws-mountpoint-s3-csi-driver ./charts/aws-mountpoint-s3-csi-driver --namespace kube-system \
    --values ./charts/aws-mountpoint-s3-csi-driver/values.yaml --values dev/mp-dev-nvme-values.yaml \
    --set unsupportedDevInstall=true --set image.pullPolicy=Always --set image.tag=latest \
    --set image.repository="$(aws ecr describe-repositories --region eu-north-1 --repository-names mp-dev --query 'repositories[0].repositoryUri' --output text)" \
    --set experimental.mounterMode=daemonset --set experimental.dynamicVolumeProvisioningFromExistingBucket=true
kubectl -n kube-system delete po -l app=s3-csi-daemonset-mounter

# Try it
kubectl apply -f examples/kubernetes/static_provisioning/jetong_cache_nvme_deployment_5.yaml

# Tear down
kubectl delete -f examples/kubernetes/static_provisioning/jetong_cache_nvme_deployment_5.yaml
./dev/mp-dev.sh deploy-helm-chart
kubectl delete -f dev/mp-dev-nvme-local-provisioner.yaml
eksctl delete nodegroup -f dev/mp-dev-nvme-nodegroup.yaml --approve
```



mp-dev-nvme-nodegroup.yaml
```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

# A node with an NVMe instance-store disk, for the `ephemeral` cache on the Local Volume Static Provisioner.
# It costs money for as long as it exists, so delete it when done:
#   eksctl create nodegroup -f dev/mp-dev-nvme-nodegroup.yaml
#   eksctl delete nodegroup -f dev/mp-dev-nvme-nodegroup.yaml --approve
metadata:
  name: mp-dev-cluster
  region: eu-north-1

managedNodeGroups:
  - name: ng-nvme
    # The cheapest type in eu-north-1 with an instance store and room for the mounter's 4Gi memory request; c5d.large has 4 GiB in all.
    instanceType: m5d.large
    desiredCapacity: 1
    amiFamily: AmazonLinux2023
    labels:
      s3.csi.aws.com/local-nvme: "true"
    iam:
      attachPolicyARNs:
        # These are required for node to function,
        # see https://eksctl.io/usage/iam-policies/#attaching-policies-by-arn
        - arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy
        - arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy
        - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPullOnly
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
        - arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy
        # These are for end-to-end tests:
        - arn:aws:iam::aws:policy/AmazonS3FullAccess
      attachPolicy:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Action:
              - "s3express:*"
            Resource: "*"
```




mp-dev-nvme-local-provisioner.yaml

```yaml
# Dev only: the Local Volume Static Provisioner, which offers each NVMe instance-store disk on a node labelled
# s3.csi.aws.com/local-nvme=true as a PersistentVolume in StorageClass nvme-ssd. Same manifest the daemonset-cache
# e2e suite installs for its NVMe row.
#   kubectl apply -f dev/mp-dev-nvme-local-provisioner.yaml
#   kubectl get pv -l storage.kubernetes.io/local-volume-owner-name   # or: kubectl get pv | grep nvme-ssd
#   kubectl delete -f dev/mp-dev-nvme-local-provisioner.yaml          # after the cache volume's claim is gone
#
# Source: kubernetes-sigs/sig-storage-local-static-provisioner helm/generated_examples/eks-nvme-ssd.yaml,
# chart local-static-provisioner-2.9.0 at commit 4d94d796, image local-volume-provisioner:v2.9.0.
# Changed from upstream:
#   - namespace default -> kube-system.
#   - discovery reads /dev/disk/by-id, which udev populates on every boot, instead of /dev/disk/kubernetes, which
#     needs a udev rule in the node's boot script. namePattern keeps the root EBS volume out and picks one of the
#     three links udev makes per instance-store disk (base, _1 and -ns-1 on AL2023), so each disk is one PV.
#   - the DaemonSet runs only on nodes labelled s3.csi.aws.com/local-nvme=true.
---
# Source: local-static-provisioner/templates/serviceaccount.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: local-static-provisioner
  namespace: kube-system
  labels:
    helm.sh/chart: local-static-provisioner-2.9.0
    app.kubernetes.io/name: local-static-provisioner
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/instance: local-static-provisioner
---
# Source: local-static-provisioner/templates/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-static-provisioner-config
  namespace: kube-system
  labels:
    helm.sh/chart: local-static-provisioner-2.9.0
    app.kubernetes.io/name: local-static-provisioner
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/instance: local-static-provisioner
data:
  storageClassMap: |
    nvme-ssd:
      hostDir: /dev/disk/by-id
      mountDir: /dev/disk/by-id
      namePattern: "nvme-Amazon_EC2_NVMe_Instance_Storage_*_1"
---
# Source: local-static-provisioner/templates/storageclass.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: nvme-ssd
  labels:
    helm.sh/chart: local-static-provisioner-2.9.0
    app.kubernetes.io/name: local-static-provisioner
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/instance: local-static-provisioner
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
---
# Source: local-static-provisioner/templates/rbac.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: local-static-provisioner-node-clusterrole
  labels:
    helm.sh/chart: local-static-provisioner-2.9.0
    app.kubernetes.io/name: local-static-provisioner
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/instance: local-static-provisioner
rules:
- apiGroups: [""]
  resources: ["persistentvolumes"]
  verbs: ["get", "list", "watch", "create", "delete"]
- apiGroups: ["storage.k8s.io"]
  resources: ["storageclasses"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["events"]
  verbs: ["watch"]
- apiGroups: ["", "events.k8s.io"]
  resources: ["events"]
  verbs: ["create", "update", "patch"]
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "update"]
---
# Source: local-static-provisioner/templates/rbac.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: local-static-provisioner-node-binding
  labels:
    helm.sh/chart: local-static-provisioner-2.9.0
    app.kubernetes.io/name: local-static-provisioner
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/instance: local-static-provisioner
subjects:
- kind: ServiceAccount
  name: local-static-provisioner
  namespace: kube-system
roleRef:
  kind: ClusterRole
  name: local-static-provisioner-node-clusterrole
  apiGroup: rbac.authorization.k8s.io
---
# Source: local-static-provisioner/templates/daemonset_linux.yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: local-static-provisioner
  namespace: kube-system
  labels:
    helm.sh/chart: local-static-provisioner-2.9.0
    app.kubernetes.io/name: local-static-provisioner
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/instance: local-static-provisioner
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: local-static-provisioner
      app.kubernetes.io/instance: local-static-provisioner
  updateStrategy:
    rollingUpdate:
      maxUnavailable: 1
    type: RollingUpdate
  template:
    metadata:
      labels:
        app.kubernetes.io/name: local-static-provisioner
        app.kubernetes.io/instance: local-static-provisioner
      annotations:
        checksum/config: ffd882b344e22fa217c669f5e3a5d2f2cdf1d049db3d2b45e934014f26b07fb8
    spec:
      hostPID: false
      serviceAccountName: local-static-provisioner
      nodeSelector:
        kubernetes.io/os: linux
        s3.csi.aws.com/local-nvme: "true"
      containers:
        - name: provisioner
          image: registry.k8s.io/sig-storage/local-volume-provisioner:v2.9.0
          securityContext:
            privileged: true
          env:
          - name: MY_NODE_NAME
            valueFrom:
              fieldRef:
                fieldPath: spec.nodeName
          - name: MY_NAMESPACE
            valueFrom:
              fieldRef:
                fieldPath: metadata.namespace
          - name: JOB_CONTAINER_IMAGE
            value: registry.k8s.io/sig-storage/local-volume-provisioner:v2.9.0
          livenessProbe:
            failureThreshold: 3
            initialDelaySeconds: 10
            periodSeconds: 60
            tcpSocket:
              port: metrics
            timeoutSeconds: 5
          ports:
          - name: metrics
            containerPort: 8080
          volumeMounts:
            - name: provisioner-config
              mountPath: /etc/provisioner/config
              readOnly: true
            - name: provisioner-dev
              mountPath: /dev
            - name: nvme-ssd
              mountPath: /dev/disk/by-id
              mountPropagation: HostToContainer
      volumes:
        - name: provisioner-config
          configMap:
            name: local-static-provisioner-config
        - name: provisioner-dev
          hostPath:
            path: /dev
        - name: nvme-ssd
          hostPath:
            path: /dev/disk/by-id
```


mp-dev-nvme-values.yaml
```yaml
# Dev only: the mounter's cache on the NVMe instance store, on the NVMe node only. Order:
#   eksctl create nodegroup -f dev/mp-dev-nvme-nodegroup.yaml      # one m5d.large, labelled s3.csi.aws.com/local-nvme=true
#   kubectl apply -f dev/mp-dev-nvme-local-provisioner.yaml        # offers its disk in StorageClass nvme-ssd
#   helm upgrade --install aws-mountpoint-s3-csi-driver ./charts/aws-mountpoint-s3-csi-driver --namespace kube-system \
#     --values ./charts/aws-mountpoint-s3-csi-driver/values.yaml --values dev/mp-dev-nvme-values.yaml \
#     --set unsupportedDevInstall=true --set image.repository=<ecr repo url> --set image.pullPolicy=Always --set image.tag=latest \
#     --set experimental.mounterMode=daemonset --set experimental.dynamicVolumeProvisioningFromExistingBucket=true
#   kubectl -n kube-system delete po -l app=s3-csi-daemonset-mounter   # the mounter DaemonSet is OnDelete
#   kubectl apply -f examples/kubernetes/static_provisioning/jetong_cache_nvme.yaml
# Undo: delete the examples, run ./dev/mp-dev.sh deploy-helm-chart (back to the default cache), delete the provisioner,
# then: eksctl delete nodegroup -f dev/mp-dev-nvme-nodegroup.yaml --approve   (the node costs money while it exists)
#
# The chart has one mounter config for every node, so this pins the driver (s3-csi-node and the mounter) to the NVMe node:
# S3 volumes on other nodes do not mount while this is deployed. s3-csi-node exits on a node without a mounter, hence both.
# A list in a values file replaces the whole list, so every field is set here.
node:
  nodeSelector:
    s3.csi.aws.com/local-nvme: "true"
daemonsetMounters:
  - maxVolumesPerNode: 5 # jetong_cache_nvme_deployment_5.yaml puts 5 volumes on the one NVMe node
    memoryLimitStrategy: equalSplit
    resources:
      requests:
        memory: "4Gi"
        cpu: "500m"
    cache:
      ephemeral:
        storageClassName: nvme-ssd
        resourceRequests: 10Gi # the local PV is the whole disk; equalSplit divides this request
      cacheLimitStrategy: equalSplit
    logLevel: 4
    podLabels: {}
    affinity: {}
    imagePullSecrets: []
```