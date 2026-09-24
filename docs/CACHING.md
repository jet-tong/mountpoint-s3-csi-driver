# Caching Configuration of Mountpoint for Amazon S3 CSI Driver

[Mountpoint for Amazon S3](https://github.com/awslabs/mountpoint-s3) supports caching file system metadata and object content to reduce cost and improve performance for repeated reads to the same file. The CSI Driver allows you to configure caching of Mountpoint in your PersistentVolume (PV) definition. See [Mountpoint's caching configuration](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#caching-configuration) for more details about caching.

## Metadata Cache

The `metadata-ttl <SECONDS|indefinite|minimal>` flag in `mountOptions` controls the time-to-live (TTL) for cached metadata entries. It can be set to a positive numerical value in seconds, or to one of the pre-configured values of `minimal` (default configuration when not using [Data Cache](#data-cache)) or `indefinite` (metadata entries never expire).

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  mountOptions:
    - metadata-ttl indefinite # <SECONDS|indefinite|minimal>
  csi:
    driver: s3.csi.aws.com
    # ...
```

See [Mountpoint's documentation](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#metadata-cache) for more details about metadata cache.

## Data Cache

Mountpoint supports different types of data caching that you can opt in to accelerate repeated read requests.

### Local Cache

The CSI Driver allows you to configure an [emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir) or a [generic ephemeral volume](https://kubernetes.io/docs/concepts/storage/ephemeral-volumes/#generic-ephemeral-volumes) as a local cache.
The CSI Driver mounts the provided cache volume to the Mountpoint Pod and configures Mountpoint to use that volume as local cache.

See [Mountpoint's documentation](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#local-cache) for more details about local cache.

> [!NOTE]
> The rest of this section describes the default (pod) mounter mode. In the experimental daemonset mode the cache volume is configured once for the whole node instead of per PV -- see [Local cache in daemonset mode](#local-cache-in-daemonset-mode).

#### `emptyDir`

You can specify `emptyDir` as cache type in your PV to use an `emptyDir` volume as local cache:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  # ...
  csi:
    driver: s3.csi.aws.com
    # ...
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
      cache: emptyDir
      cacheEmptyDirSizeLimit: 2Gi # optional but highly recommended!
      cacheEmptyDirMedium: Memory # optional
```

Both `cacheEmptyDirSizeLimit` and `cacheEmptyDirMedium` are optional, but we highly recommend you specify a size limit on your cache, as it might otherwise use all your node's storage depending on the cluster's configuration.

`cacheEmptyDirMedium` controls the storage medium for the `emptyDir` volume. If not specified, the cache uses the node's default medium (disk, SSD, or network storage). Set it to `Memory` to use a `tmpfs` ramdisk instead, which offers lower latency at the cost of consuming node memory.

`cacheEmptyDirSizeLimit` sets the maximum size of the `emptyDir` volume. We highly recommend setting this to avoid the cache consuming all available storage on the node. For disk-backed (default) medium, the CSI Driver will automatically enforce that limit in Mountpoint to prevent the Mountpoint Pod from being evicted by Kubernetes. You can override this by setting `max-cache-size` in `mountOptions`, but it must not exceed `cacheEmptyDirSizeLimit`.

The `emptyDir` will be unique to each Mountpoint Pod and won't be shared between other Mountpoint instances.

See [Kubernetes's documentation](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir) for more details about `emptyDir`.

#### `ephemeral`

You can specify `ephemeral` as cache type alongside a StorageClass and storage size in your PV to use a generic ephemeral volume as local cache:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  # ...
  csi:
    driver: s3.csi.aws.com
    # ...
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
      cache: ephemeral
      cacheEphemeralStorageClassName: gp2 # required
      cacheEphemeralStorageResourceRequest: 4Gi # required
```

The CSI Driver will create a PersistentVolumeClaim (PVC) template within the Mountpoint Pod's volumes using the configured values and [`ReadWriteOnce` access mode](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes) to get a unique PVC created for the Mountpoint Pod.
Both `cacheEphemeralStorageClassName` and `cacheEphemeralStorageResourceRequest` are required to specify a StorageClass name and a storage size to request from the StorageClass respectively.

Using the `ephemeral` cache type, you can use the [Amazon Elastic Block Store (EBS) CSI driver](https://github.com/kubernetes-sigs/aws-ebs-csi-driver) to dynamically provision an EBS volume or use [Local Volume Static Provisioner](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner) to access your [Amazon EC2 Instance Store](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html). See examples below for more details.

##### Using EBS CSI Driver to provision an EBS volume dynamically

First, make sure to install the EBS CSI Driver in your cluster by following their [installation guide](https://github.com/kubernetes-sigs/aws-ebs-csi-driver/blob/master/docs/install.md).

You can then create a StorageClass using the EBS CSI Driver for Mountpoint CSI Driver to request a volume to use as local cache:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: s3-cache-ebs-sc
provisioner: ebs.csi.aws.com
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
parameters: # all optional, see https://github.com/kubernetes-sigs/aws-ebs-csi-driver/blob/master/docs/parameters.md for more details
  type: io2
  iopsPerGB: "256000"
  blockExpress: "true"
```

You can then reference this StorageClass from your S3 PV:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  # ...
  csi:
    driver: s3.csi.aws.com
    # ...
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
      cache: ephemeral
      cacheEphemeralStorageClassName: s3-cache-ebs-sc
      cacheEphemeralStorageResourceRequest: 10Gi
```

With this configuration, once your workload is scheduled onto a node, Mountpoint CSI Driver will schedule a Mountpoint Pod to the same node with the `ephemeral` volume. EBS CSI Driver will then dynamically provision an EBS volume and attach it to the node for Mountpoint to use as cache.

The EBS volume and the Mountpoint Pod (therefore its ephemeral PVC) will be automatically cleaned up once the workload is terminated. We highly recommend you use `reclaimPolicy: Delete` in your StorageClass to ensure the cache PV is automatically cleaned up as part of this process.

##### Using Local Volume Static Provisioner to use local NVMe

Some Amazon EC2 instances offer non-volatile memory express (NVMe) solid state drives (SSD) instance store volumes. You can utilize [Local Volume Static Provisioner](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner) to use instance store as cache. See [Instance store volume limits for EC2 instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-store-volumes.html) for more details about instance store support on EC2 instances, and [EKS Persistent Volumes for Instance Store](https://aws.amazon.com/blogs/containers/eks-persistent-volumes-for-instance-store/) on using instance storage in EKS.

The Local Volume Static Provisioner allows you to configure various options. You can find more details in their [Getting started guide](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner/blob/master/docs/getting-started.md).

As an example, you can configure your [eksctl](https://eksctl.io/) configuration to mount available NVMe instance storage disks at `/dev/disk/kubernetes`:

```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: cluster-with-storage
  region: eu-central-1
managedNodeGroups:
  - name: storage-nvme
    desiredCapacity: 2
    instanceType: i3.8xlarge
    amiFamily: AmazonLinux2023
    preBootstrapCommands:
      - |
          cat <<EOF > /etc/udev/rules.d/90-kubernetes-discovery.rules
          # Discover Instance Storage disks so kubernetes local provisioner can pick them up from /dev/disk/kubernetes
          KERNEL=="nvme[0-9]*n[0-9]*", ENV{DEVTYPE}=="disk", ATTRS{model}=="Amazon EC2 NVMe Instance Storage", ATTRS{serial}=="?*", SYMLINK+="disk/kubernetes/nvme-\\\$attr{model}_\\\$attr{serial}", OPTIONS="string_escape=replace"
          EOF
      - udevadm control --reload && udevadm trigger
```

The `i3.8xlarge` instance type provides four NVMe instance storage disks. After applying your changes using `eksctl`, you can install the example EKS NVMe manifest:
```bash
$ kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/sig-storage-local-static-provisioner/refs/heads/master/helm/generated_examples/eks-nvme-ssd.yaml
```

This should create a StorageClass named `nvme-ssd` and eight PVs for each local NVMe instance storage disk attached to two instances (four for each instance):

```bash
$ kubectl get sc nvme-ssd
NAME       PROVISIONER                    RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
nvme-ssd   kubernetes.io/no-provisioner   Delete          WaitForFirstConsumer   false                  17s

$ kubectl get pv
NAME                CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS      CLAIM   STORAGECLASS   VOLUMEATTRIBUTESCLASS   REASON   AGE
local-pv-12305867   1769Gi     RWO            Delete           Available           nvme-ssd       <unset>                          60s
local-pv-12342524   1769Gi     RWO            Delete           Available           nvme-ssd       <unset>                          60s
local-pv-30a97d4d   1769Gi     RWO            Delete           Available           nvme-ssd       <unset>                          60s
local-pv-5a838bd7   1769Gi     RWO            Delete           Available           nvme-ssd       <unset>                          60s
local-pv-743f383d   1769Gi     RWO            Delete           Available           nvme-ssd       <unset>                          49s
local-pv-dae2484    1769Gi     RWO            Delete           Available           nvme-ssd       <unset>                          49s
local-pv-ea190b38   1769Gi     RWO            Delete           Available           nvme-ssd       <unset>                          49s
local-pv-ef5d9823   1769Gi     RWO            Delete           Available           nvme-ssd       <unset>                          49s
```

You can now specify StorageClass `nvme-ssd` in your PV's configuration with the `ephemeral` cache type:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  # ...
  csi:
    driver: s3.csi.aws.com
    # ...
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
      cache: ephemeral
      cacheEphemeralStorageClassName: nvme-ssd
      cacheEphemeralStorageResourceRequest: 10Gi
```

One thing to note is that, since the local NVMe instance storage disks are local to the nodes,
you need to ensure your workload and therefore the Mountpoint Pod is scheduled onto a node with local NVMe and associated PV available.
You can use [`nodeSelector`](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector) or [Node affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity) rules to achieve that.

For example, this configuration would ensure that your workload is scheduled on a node from the `eksctl` node group `storage-nvme`:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: workload
spec:
  containers:
    # ...
  volumes:
    - name: vol
      persistentVolumeClaim:
        claimName: s3-pvc
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: alpha.eksctl.io/nodegroup-name
                operator: In
                values:
                  - storage-nvme
          # OR using node name
          # - matchExpressions:
          #     - key: kubernetes.io/hostname
          #       operator: In
          #       values:
          #         - ip-192-0-2-0.region-code.compute.internal
```

After deploying your workload, the Mountpoint Pod should also be deployed to the same node automatically with a local NVMe PV attached to it:
```bash
$ kubectl describe po -n mount-s3
Name:                 mp-ql5rd
Namespace:            mount-s3
...
Volumes:
  ...
  local-cache:
    Type:          EphemeralVolume (an inline specification for a volume that gets created and deleted with the pod)
    StorageClass:  nvme-ssd
    Volume:
    Labels:            s3.csi.aws.com/type=local-ephemeral-cache
    Annotations:       <none>
    Capacity:
    Access Modes:
    VolumeMode:    Filesystem

$ kubectl describe pvc -n mount-s3
Name:          mp-xt6c4-local-cache
Namespace:     mount-s3
StorageClass:  nvme-ssd
Status:        Bound
Volume:        local-pv-743f383d
Labels:        s3.csi.aws.com/type=local-ephemeral-cache
Annotations:   pv.kubernetes.io/bind-completed: yes
               pv.kubernetes.io/bound-by-controller: yes
Finalizers:    [kubernetes.io/pvc-protection]
Capacity:      1769Gi
Access Modes:  RWO
VolumeMode:    Filesystem
Used By:       mp-ql5rd
```

Note that if there is no local NVMe available in the scheduled node, the Mountpoint Pod would fail to schedule and your workload would hang in `Pending` state. You can `kubectl describe pods -n mount-s3` to describe your Mountpoint Pod to see if it has any unsatisfied deployment requirements. The CSI Driver would emit an helpful error message for you to check your Mountpoint Pod's status in this case.

You must ensure your workload (and therefore the Mountpoint Pod) is scheduled to a node with local NVMe available to use.

Ensure you check [other configurations of Local Volume Static Provisioner](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner/tree/master?tab=readme-ov-file#user-guide) including
[Local Volume Node Cleanup Controller](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner/blob/master/docs/node-cleanup-controller.md) for volume cleanup and other details.

#### (Deprecated) `cache` flag via `mountOptions`

With the CSI Driver v1, the Mountpoint instances were spawned on the host using `systemd`, and the `cache` flag in `mountOptions` was a relative path to the host. The cache folder also needed to exist for Mountpoint to use. We have deprecated this usage and will fallback to using [`emptyDir`](#emptyDir) with the default storage medium without any limit by default.

You no longer need to create a cache folder on the host, and the configured path will be ignored by the CSI Driver v2! We recommend customers migrate to [`emptyDir`](#emptyDir) and specify a limit.

For this deprecated use of the cache configuration:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  mountOptions:
    - cache /cache/folder/on/host
  csi:
    driver: s3.csi.aws.com
    # ...
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
```

the CSI Driver will ignore the cache path and will create an `emptyDir` cache volume instead. The end result will be the same as this configuration:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  # ...
  csi:
    driver: s3.csi.aws.com
    # ...
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
      cache: emptyDir
```

#### Local cache in daemonset mode

<!-- TODO Remove warning -->
> [!WARNING]
> Daemonset mode is experimental and its configuration may change between releases.

In daemonset mode every Mountpoint process on a node runs in the shared `s3-csi-daemonset-mounter` pod, so there is no per-mount pod to attach a cache volume to. Instead one cache volume is configured for the whole node and each mount is given its own subdirectory of it. Enable it with the `daemonsetMounters[0].cache` Helm value:

```yaml
daemonsetMounters:
  - maxVolumesPerNode: 4
    # Adding this block gives the node a cache volume. Remove it to disable caching.
    cache:
      # "emptyDir" (node disk or RAM) or "ephemeral" (dedicated volume from your StorageClass).
      type: emptyDir
      emptyDir:
        medium: ""
      # type: ephemeral
      # ephemeral:
      #   storageClassName: ebs-sc
      #   size: 100Gi
```

The `cache.type` values correspond to the per-PV [`emptyDir`](#emptydir) and [`ephemeral`](#ephemeral) types described above, and the same considerations apply when choosing between them.

PVs then opt in without configuring storage, since the volume already exists:

```yaml
apiVersion: v1
kind: PersistentVolume
spec:
  mountOptions:
    # Optional: limit how much of the shared cache volume this mount may use.
    - max-cache-size 10240
  csi:
    driver: s3.csi.aws.com
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
      # Must match the node's cache type, set by the Helm value above.
      cache: emptyDir
```

##### The PV's cache type must match the node's

The cache backing is a property of the node, not of the volume, so a PV cannot choose one. It must instead name the backing the node already has, and the mount is rejected if it does not:

| Helm `cache` block | The PV must set |
|---|---|
| `type: emptyDir`, `emptyDir.medium: ""` | `cache: emptyDir` |
| `type: emptyDir`, `emptyDir.medium: Memory` | `cache: emptyDir` **and** `cacheEmptyDirMedium: Memory` |
| `type: ephemeral` | `cache: ephemeral` |

`cacheEmptyDirMedium` is the one per-PV cache attribute daemonset mode still reads, because it selects a backing rather than a size. The mount fails with `InvalidArgument` when the PV requests a backing the node does not provide, names a value that is not a cache type (including `true`), or requests a cache when the node has none. The message reports both what was requested and what the node provides.

A PV that enables the cache with the deprecated `mountOptions: cache <dir>` names no type, so it accepts whatever the node provides. This is the only case where the type is not checked.

Mounting a volume that requests a cache fails if the `cache` block is absent, so add it before creating such PVs.

Adding or removing the block changes the DaemonSet's pod spec, and the mounter DaemonSet uses the `OnDelete` update strategy, so existing pods keep running until you delete them. Deleting a mounter pod terminates every Mountpoint process on that node, so drain the node first:

```bash
kubectl cordon <node>
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data --force
kubectl delete pod -n kube-system -l app=s3-csi-daemonset-mounter --field-selector spec.nodeName=<node>
kubectl uncordon <node>
```

`--delete-emptydir-data` is required because the mounter pod uses `emptyDir` volumes, which `kubectl drain` will not evict otherwise; see [My node drain fails with "cannot delete Pods that declare no controller"](./TROUBLESHOOTING.md#my-node-drain-fails-with-cannot-delete-pods-that-declare-no-controller) for the details and for the PodDisruptionBudget caveat.

> [!IMPORTANT]
> **Changing `cache.type` or `emptyDir.medium` breaks PVs pinned to the old one.** Because a PV names the node's backing, changing the node's backing makes every PV naming the old one fail to mount. Update those PVs first -- and since `volumeAttributes` are immutable, that means deleting and recreating them, and therefore the workloads bound to them.

> [!IMPORTANT]
> **Removing the cache is not simply the reverse.** A PV's `volumeAttributes` are immutable, so a PV that requests a cache cannot be edited to stop requesting one. If you remove the `cache` block from `values.yaml` while such PVs still exist, their mounts fail and the driver retries indefinitely. Remove the `cache` volume attribute from every PV first -- which means deleting and recreating those PVs, and therefore the workloads bound to them -- and only then remove the Helm block.

The volume attributes that configure a per-mount cache volume in pod mode (`cacheEmptyDirSizeLimit`, `cacheEmptyDirMedium`, `cacheEphemeralStorageClassName`, `cacheEphemeralStorageResourceRequest`) have no effect in daemonset mode. The driver logs a warning if a PV sets them.

##### Sizing the cache volume

Size the cache volume for the number of mounts you expect on a node, in the same way as the mounter pod's memory request: for `maxVolumesPerNode` mounts each using at most `max-cache-size`, the volume needs `maxVolumesPerNode x max-cache-size`.

Nothing enforces this total. Each Mountpoint process only limits its own cache, so mounts that collectively request more than the volume holds will fill it:

- With `type: ephemeral` the volume is a separate filesystem, so filling it only affects caching -- Mountpoint logs a warning and reads fall through to S3.
- With `type: emptyDir` the cache shares the node's filesystem with container images, logs, and other pods. Filling it can put the node under disk pressure, which causes the kubelet to evict pods. **Set `max-cache-size` on your PVs when using `emptyDir`**, or use `type: ephemeral` to keep cache growth away from the node's disk.

Note that `emptyDir.sizeLimit` bounds this cache only with `medium: Memory`, where it sizes the tmpfs and the kernel enforces it. On the node's disk it bounds nothing: the kubelet enforces `sizeLimit` by evicting the pod that exceeds it, and it does not evict `system-node-critical` pods such as the mounter DaemonSet.

### Shared Cache

When mounting an S3 bucket, you can opt in to a shared cache in [Amazon S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/). You should use the shared cache if you repeatedly read small objects (up to 1 MB) from multiple compute instances, or the size of the dataset that you repeatedly read often exceeds the size of your local cache. This improves latency when reading the same data repeatedly from multiple instances by avoiding redundant requests to your mounted S3 bucket. To enable shared cache, specify the `cache-xz` flag in `mountOptions` with your directory bucket name:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  mountOptions:
    - cache-xz amzn-s3-demo-bucket--usw2-az1--x-s3
  csi:
    driver: s3.csi.aws.com
    # ...
```

See [Mountpoint's documentation](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#shared-cache) for more details about shared cache.

### Combined Local and Shared Cache

You can opt in to a local cache and shared cache together if you have unused space on your instance, but also want to share the cache across multiple instances. This avoids redundant read requests from the same instance to the shared cache in S3 directory bucket when the required data is cached in local storage, reducing request cost as well as improving performance. To opt in to local and shared cache together, you can specify both the [Local Cache](#local-cache) and [Shared Cache](#shared-cache) in your PV:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  mountOptions:
    - cache-xz amzn-s3-demo-bucket--usw2-az1--x-s3
  csi:
    driver: s3.csi.aws.com
    # ...
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
      cache: emptyDir
      cacheEmptyDirSizeLimit: 2Gi
```

See [Mountpoint's documentation](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#combined-local-and-shared-cache) for more details about combined local and shared cache.
