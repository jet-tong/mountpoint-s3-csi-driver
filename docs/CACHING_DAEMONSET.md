# Caching with Mountpoint for Amazon S3 CSI Driver v3

[Mountpoint for Amazon S3](https://github.com/awslabs/mountpoint-s3) can cache metadata and object content, so repeated reads of the same data are faster and cost less. This page covers caching in v3, where the driver runs Mountpoint in a daemonset mounter. Upgrading from v2? See [Upgrading to v3](UPGRADING_TO_V3.md).

**In short:**

1. Give each node one cache volume, in your Helm values.
2. Pick how that volume is shared between mounts: `equalSplit` (recommended) or `none`.
3. Set `cache: enabled` on each PV that should use it.

- [How the cache works in v3](#how-the-cache-works-in-v3)
- [Step 1: Give each node a cache volume](#step-1-give-each-node-a-cache-volume)
- [Step 2: Choose how the volume is shared](#step-2-choose-how-the-volume-is-shared)
- [Step 3: Turn the cache on for a PV](#step-3-turn-the-cache-on-for-a-pv)
- [Metadata cache](#metadata-cache)
- [Shared cache (S3 Express One Zone)](#shared-cache-s3-express-one-zone)
- [Changing the cache configuration](#changing-the-cache-configuration)
- [Troubleshooting](#troubleshooting)

---

## How the cache works in v3

In v3, every Mountpoint process on a node runs inside one pod, `s3-csi-daemonset-mounter`. So there is **one cache volume per node**, mounted in that pod at `/cache`, and **each PV gets its own directory** in it:

```
/cache/<pv-a>/    <- the Mountpoint for PV a
/cache/<pv-b>/    <- the Mountpoint for PV b
```

The driver creates a PV's directory when it is first mounted on the node, and deletes it when the last pod using that PV on the node goes away.

| | v2 | v3 |
|---|---|---|
| Where the cache volume is configured | on each PV (`volumeAttributes`) | once, in Helm values (`daemonsetMounters[0].cache`) |
| How many cache volumes | one per Mountpoint pod | one per node, shared |
| How a PV opts in | `cache: emptyDir` or `cache: ephemeral`, plus sizes | `cache: enabled` |
| What bounds one mount's cache | its own volume size | `cacheLimitStrategy` (see [Step 2](#step-2-choose-how-the-volume-is-shared)) |

---

## Step 1: Give each node a cache volume

Uncomment **exactly one** of these blocks under `daemonsetMounters[0]` in your Helm values.

| Cache volume | Use it when | Block |
|---|---|---|
| Node disk | default choice; no extra cost | `emptyDir` |
| Memory (tmpfs) | lowest latency, small hot datasets | `emptyDir` with `medium: "Memory"` |
| EBS volume | you want the cache off the node's root disk | `ephemeral` with an EBS StorageClass |
| Instance store (NVMe) | large caches on instances with local NVMe | `ephemeral` with a local-volume StorageClass |

### Node disk

```yaml
daemonsetMounters:
  - maxVolumesPerNode: 4
    cache:
      emptyDir:
        sizeLimit: "10Gi"
      cacheLimitStrategy: equalSplit
```

> [!IMPORTANT]
> On the node's disk, `sizeLimit` alone does **not** stop the cache growing. The kubelet enforces `sizeLimit` by evicting the pod, and it never evicts the mounter, which is `system-node-critical`. Mountpoint's own "keep 5% free" check measures the node's whole root filesystem, not this volume. Use `cacheLimitStrategy: equalSplit` so each mount has a real bound.

### Memory (tmpfs)

```yaml
daemonsetMounters:
  - maxVolumesPerNode: 4
    resources:
      requests:
        memory: "6Gi"        # raised by sizeLimit, see below
    cache:
      emptyDir:
        sizeLimit: "2Gi"
        medium: "Memory"
      cacheLimitStrategy: equalSplit
```

A tmpfs is memory. Its pages count against the mounter pod's memory, the same budget Mountpoint uses for its buffers.

- **Raise `resources.requests.memory` by `sizeLimit`.**
- With `memoryLimitStrategy: equalSplit`, the driver subtracts `sizeLimit` from the memory request before it divides the rest between mounts. The chart refuses to install if that leaves a mount less than Mountpoint's 512 MiB minimum.
- Always set `sizeLimit`. Without it, the kubelet sizes the tmpfs at the node's allocatable memory. `cacheLimitStrategy: none` lets you leave it out, but then nothing bounds the tmpfs unless every PV sets `max-cache-size`, and it is not subtracted from the memory split: a cache that grows too large can get the mounter OOM-killed or evicted, taking every mount on the node with it.

### EBS volume

Install the [Amazon EBS CSI driver](https://github.com/kubernetes-sigs/aws-ebs-csi-driver/blob/master/docs/install.md), then create a StorageClass:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: s3-cache-ebs-sc
provisioner: ebs.csi.aws.com
reclaimPolicy: Delete                    # recommended: the volume goes when the mounter pod goes
volumeBindingMode: WaitForFirstConsumer
parameters:                              # optional, see the EBS CSI driver's parameters doc
  type: gp3
```

```yaml
daemonsetMounters:
  - maxVolumesPerNode: 4
    cache:
      ephemeral:
        storageClassName: s3-cache-ebs-sc
        resourceRequests: 40Gi
      cacheLimitStrategy: equalSplit
```

Each mounter pod gets its own EBS volume, provisioned when the pod starts and deleted with it.

### Instance store (NVMe)

Some EC2 instances have local NVMe instance store. The [Local Volume Static Provisioner](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner) turns those disks into PVs that the cache can claim.

1. Expose the disks at `/dev/disk/kubernetes` when the node boots. With `eksctl`:

   ```yaml
   managedNodeGroups:
     - name: storage-nvme
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

2. Install the provisioner. Its EKS example creates a StorageClass named `nvme-ssd`, with one PV per disk:

   ```bash
   kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/sig-storage-local-static-provisioner/refs/heads/master/helm/generated_examples/eks-nvme-ssd.yaml
   kubectl get pv    # expect Available PVs in StorageClass nvme-ssd
   ```

3. Point the cache at it:

   ```yaml
   daemonsetMounters:
     - maxVolumesPerNode: 4
       cache:
         ephemeral:
           storageClassName: nvme-ssd
           resourceRequests: 100Gi
         cacheLimitStrategy: equalSplit
   ```

> [!IMPORTANT]
> Every node running the mounter needs a free local PV in `nvme-ssd`. On a node without one, the mounter pod stays `Pending`, and **no** S3 volume can mount on that node, cached or not.

<!-- TODO: document per-node-group cache configuration once heterogeneous daemonsetMounters are supported. -->

See also the provisioner's [node cleanup controller](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner/blob/master/docs/node-cleanup-controller.md), which cleans up local PVs after a node is removed.

---

## Step 2: Choose how the volume is shared

All mounts on a node share one cache volume. `cacheLimitStrategy`, inside the `cache` block, decides how much of it each mount may use. It is required.

| | `equalSplit` (recommended) | `none` |
|---|---|---|
| Each mount's cache limit | `95% × volume size ÷ maxVolumesPerNode` | the PV's own `max-cache-size` |
| `max-cache-size` on a PV | ignored, with a warning in the mounter's log | used as-is |
| A PV with no `max-cache-size` | gets the equal share | **no limit** (a warning in the mounter's log) |
| Requires | a volume size, and `maxVolumesPerNode` above 0 | nothing |
| Can one mount fill the volume? | no | yes, unless every PV sets `max-cache-size` |

`equalSplit` examples, with `maxVolumesPerNode: 4`:

| Cache volume | Each mount gets |
|---|---|
| `emptyDir.sizeLimit: 4Gi` | 972 MiB |
| `emptyDir.sizeLimit: 10Gi` | 2432 MiB |
| `ephemeral.resourceRequests: 40Gi` | 9728 MiB |

The 5% headroom exists because Mountpoint evicts cache entries only after a write has already gone over its limit.

**Use `none` only if** your mounts need different cache sizes, and then set `max-cache-size` (in MiB) on **every** PV that caches:

```yaml
spec:
  mountOptions:
    - max-cache-size 5000
```

---

## Step 3: Turn the cache on for a PV

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: s3-pv
spec:
  # ...
  csi:
    driver: s3.csi.aws.com
    volumeHandle: s3-csi-driver-volume
    volumeAttributes:
      bucketName: amzn-s3-demo-bucket
      cache: enabled
```

| `cache` value | Result |
|---|---|
| unset, or `disabled` | no cache |
| `enabled` | uses the node's cache |
| `"emptyDir"` or `"ephemeral"` (v2 values) | uses the node's cache, **whatever its type** |
| anything else | the mount fails, naming the value |

- Both are matched in any case (`Enabled` works too) and need no quotes. `true` and `false` are not accepted: unquoted, YAML makes them booleans, which the API server refuses.
- A PV cannot pick a cache type in v3. It uses whichever cache the node has.
- `cacheEmptyDirMedium`, `cacheEmptyDirSizeLimit`, `cacheEphemeralStorageClassName` and `cacheEphemeralStorageResourceRequest` are ignored.
- The v1 `cache <dir>` mount option still turns the cache on. Its path is ignored, and the driver logs a warning. Use `cache: enabled` instead, and never set both.

---

## Metadata cache

Unchanged from v2. `metadata-ttl` in `mountOptions` sets how long cached metadata stays valid: a number of seconds, `minimal`, or `indefinite`.

```yaml
spec:
  mountOptions:
    - metadata-ttl indefinite
```

When a PV uses the local cache, Mountpoint's default metadata TTL is 60 seconds rather than `minimal`. See [Mountpoint's metadata cache docs](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#metadata-cache).

---

## Shared cache (S3 Express One Zone)

Unchanged from v2, and it needs no cache volume. Add `cache-xz` with an S3 directory bucket to `mountOptions`:

```yaml
spec:
  mountOptions:
    - cache-xz amzn-s3-demo-bucket--usw2-az1--x-s3
```

It helps when many instances repeatedly read the same small objects, or when your working set is bigger than a local cache. To use it **with** the local cache, add `cache: enabled` to the same PV. Mountpoint then reads from local cache first, and from the shared cache next. See [Mountpoint's shared cache docs](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#shared-cache).

---

## Changing the cache configuration

The mounter DaemonSet uses the `OnDelete` update strategy, so `helm upgrade` alone does not change running mounter pods.

1. `helm upgrade` with the new `cache` values.
2. On each node, when no workload needs S3 volumes (for example after draining the node), delete the mounter pod. It runs in the driver's namespace, `kube-system` by default:

   ```bash
   kubectl delete pod -n kube-system -l app=s3-csi-daemonset-mounter --field-selector spec.nodeName=<node>
   ```

Deleting a mounter pod stops every Mountpoint process on that node, so do it only on drained nodes.

One `maxVolumesPerNode` value serves two purposes: the scheduler uses it to cap S3 volumes per node, and `equalSplit` divides the cache by it. After changing it, restart the mounter pods as above, so the shares match the new cap.

---

## Troubleshooting

Mount errors show up as events on the workload pod (`kubectl describe pod <workload>`). Warnings about cache sizing are in the mounter's log, in the namespace the driver is installed in (`kube-system` by default):

```bash
kubectl logs -n kube-system -l app=s3-csi-daemonset-mounter --tail=200
```

| You see | Why | Fix |
|---|---|---|
| `...requests a local cache, but s3-csi-daemonset-mounter has no cache volume` | the PV set `cache` but the node has no cache block | add `daemonsetMounters[0].cache` and restart the mounter pods, or remove `cache` from the PV |
| `...sets the "cache" volume attribute to "ture"` | an unrecognised value | set `cache: enabled` or `disabled` |
| `Cache configured with both mountOptions and volumeAttributes` | the PV has the v1 `cache <dir>` mount option **and** a `cache` attribute | remove the mount option |
| the mount fails with a Mountpoint error about `--max-cache-size` needing `--cache` | `max-cache-size` is set but the PV doesn't turn the cache on | add `cache: enabled`, or remove `max-cache-size` |
| Helm: `cache must set exactly one of emptyDir or ephemeral` | both blocks, neither, or an empty `cache:` | keep exactly one |
| Helm: `cache.cacheLimitStrategy must be one of [equalSplit none]` | the strategy is missing or misspelled | set `cacheLimitStrategy` inside `cache` |
| Helm: `...gives each Mountpoint process no whole MiB of --max-cache-size` | the volume is too small for `maxVolumesPerNode` mounts | raise the size or lower `maxVolumesPerNode` |
| Helm: `...tmpfs cache volume, which split across maxVolumesPerNode=... below Mountpoint's minimum --memory-target of 512 MiB` | the tmpfs leaves too little memory | raise `resources.requests.memory` or lower `sizeLimit` |
| mounter log: `Ignoring --max-cache-size=... for volume ...` | `equalSplit` replaces a PV's own size | remove `max-cache-size` from the PV, or use `none` |
| mounter log: `...so nothing bounds its share of the cache volume` | `none`, and the PV has no `max-cache-size` | add `max-cache-size` to the PV |
| workloads stuck `ContainerCreating` on one node, mounter pod `Pending` | an `ephemeral` cache whose StorageClass has no volume for this node | check `kubectl describe pod -n kube-system <mounter pod>` and the StorageClass |
