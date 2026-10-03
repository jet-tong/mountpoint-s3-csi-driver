# Upgrading Mountpoint for Amazon S3 CSI Driver to v3

In v2, each mount runs in its own Mountpoint pod. In v3, every Mountpoint process on a node runs in one daemonset mounter pod, `s3-csi-daemonset-mounter`. Most of the changes below follow from that.

<!-- TODO: add the other v3 changes (memory limits, volume limits, logs, credentials) as they are finalised. This page currently covers the local cache. -->

- [What changes for the local cache](#what-changes-for-the-local-cache)
- [Before you upgrade: find the PVs that cache](#before-you-upgrade-find-the-pvs-that-cache)
- [Upgrade with `equalSplit` (recommended)](#upgrade-with-equalsplit-recommended)
- [Upgrade with `none`](#upgrade-with-none)
- [After the upgrade](#after-the-upgrade)
- [FAQ](#faq)

---

## What changes for the local cache

In v2, a PV described its own cache volume. In v3, **each node has one cache volume, set in Helm values**, and a PV only turns the cache on. See [Caching in v3](CACHING_DAEMONSET.md) for the full guide.

| Your v2 PV has | In v3 |
|---|---|
| `cache: emptyDir` or `cache: ephemeral` | still turns the cache on, using **whichever cache the node has** |
| `cacheEmptyDirMedium` | ignored: the node's cache decides the medium |
| `cacheEmptyDirSizeLimit`, `cacheEphemeralStorageResourceRequest` | ignored: they **no longer limit the cache** |
| `cacheEphemeralStorageClassName` | ignored: the node's cache decides the StorageClass |
| `mountOptions: max-cache-size N` | used under `cacheLimitStrategy: none`; ignored under `equalSplit` |
| `mountOptions: cache <dir>` (v1 style) | still turns the cache on; the path is ignored |

**Nothing in an existing v2 PV makes it fail in v3.** What changes is how much cache each mount may use. That is set by `cacheLimitStrategy`, so choose one of the two paths below.

<!-- TODO: decide whether v3 should fall back to 95% of cacheEmptyDirSizeLimit / cacheEphemeralStorageResourceRequest when a PV sets no max-cache-size under `none`. Until then, set max-cache-size explicitly, as the `none` path below does. -->

| | `equalSplit` (recommended) | `none` |
|---|---|---|
| Each mount's cache limit | an equal share of the node's cache volume | the PV's `max-cache-size` |
| PV changes before the upgrade | none | add `max-cache-size` to every PV that caches |
| Choose it when | your mounts use similar amounts of cache | a few mounts need much more cache than others |

---

## Before you upgrade: find the PVs that cache

This lists every S3 PV that turns the cache on, with the size settings v3 treats differently (requires `jq`):

```bash
kubectl get pv -o json | jq -r '
  ["PV", "CACHE", "MOUNT OPTIONS", "V2 SIZE"],
  (.items[]
   | select(.spec.csi.driver == "s3.csi.aws.com")
   | select(.spec.csi.volumeAttributes.cache != null or any(.spec.mountOptions[]?; startswith("cache ")))
   | [.metadata.name,
      (.spec.csi.volumeAttributes.cache // "-"),
      ([.spec.mountOptions[]? | select(startswith("cache ") or startswith("max-cache-size"))] | join(", ") | if . == "" then "-" else . end),
      (.spec.csi.volumeAttributes.cacheEmptyDirSizeLimit // .spec.csi.volumeAttributes.cacheEphemeralStorageResourceRequest // "-")])
  | @tsv' | column -t -s $'\t'
```

Keep this list: you need it for either path.

---

## Upgrade with `equalSplit` (recommended)

No PV changes are needed first. Every mount on a node gets an equal share of the node's cache volume.

1. **Size the node's cache volume.** A useful starting point: roughly what your largest v2 PVs asked for (the `V2 SIZE` column) × `maxVolumesPerNode`, since each mount gets about a `1 / maxVolumesPerNode` share.

2. **Add the cache to your Helm values**, with exactly one of `emptyDir` or `ephemeral`:

   ```yaml
   daemonsetMounters:
     - maxVolumesPerNode: 4
       cache:
         emptyDir:
           sizeLimit: "10Gi"        # 4 mounts get 2432 MiB each
         cacheLimitStrategy: equalSplit
   ```

   See [Step 1 of the caching guide](CACHING_DAEMONSET.md#step-1-give-each-node-a-cache-volume) for tmpfs, EBS and instance-store volumes.

3. **Upgrade the driver** to v3 with these values.

4. **Later, tidy your PVs** (optional). Change `cache` to `enabled` and remove the four v2 cache attributes. A PV's `volumeAttributes` can't be edited in place, so this means recreating the PV and its PVC, which you can do as each workload is next redeployed.

---

## Upgrade with `none`

Each PV keeps its own limit. **v3 no longer reads `cacheEmptyDirSizeLimit` or `cacheEphemeralStorageResourceRequest`**, so a PV that relied on those would have no limit after the upgrade. Give every caching PV an explicit `max-cache-size` first.

1. **Add `max-cache-size` (in MiB) to every PV in your list that doesn't have one.** To keep v2's behaviour for a disk `emptyDir`, use 95% of its `cacheEmptyDirSizeLimit`: for `2Gi`, that's `max-cache-size 1945`. A PV's `mountOptions` can be edited in place:

   ```bash
   kubectl patch pv s3-pv --type json \
     -p '[{"op": "add", "path": "/spec/mountOptions/-", "value": "max-cache-size 1945"}]'
   ```

   If the PV has no `mountOptions` yet, use `"path": "/spec/mountOptions", "value": ["max-cache-size 1945"]` instead. The new option takes effect the next time the volume is mounted.

2. **Add the cache to your Helm values** with `none`:

   ```yaml
   daemonsetMounters:
     - maxVolumesPerNode: 4
       cache:
         emptyDir:
           sizeLimit: "20Gi"        # optional under none, but it sizes the volume
         cacheLimitStrategy: none
   ```

   The PVs' `max-cache-size` values on one node should add up to less than the cache volume. Nothing enforces that under `none`.

3. **Upgrade the driver** to v3 with these values.

4. **Later, tidy your PVs** (optional). Change `cache` to `enabled` and remove the four v2 cache attributes, by recreating each PV when its workload is next redeployed.

---

## After the upgrade

The mounter logs the limit it resolved when it starts:

```bash
kubectl logs -n kube-system -l app=s3-csi-daemonset-mounter | grep cacheLimitStrategy
```

Under `equalSplit` you should see a line like `cacheLimitStrategy=equalSplit: each Mountpoint gets --max-cache-size=2432 ...`. Warnings name any PV whose own `max-cache-size` is ignored (`equalSplit`), or that has no limit at all (`none`).

---

## FAQ

**Do I have to change my PVs?**
No. v2 PVs with `cache: emptyDir` or `cache: ephemeral` keep working. Under `none`, add `max-cache-size` first, as above.

**Can different PVs on one node use different cache types, e.g. one tmpfs and one disk?**
No. A node has one cache volume and every caching PV on it uses that. A v2 PV that asked for `cacheEmptyDirMedium: Memory` gets the node's cache whatever it is.

<!-- TODO: link heterogeneous (per node group) mounter configuration once supported. -->

**What happens to a PV that turns the cache on, on a node with no cache volume?**
The mount fails with `...requests a local cache, but s3-csi-daemonset-mounter has no cache volume`. Add a `cache` block, or remove `cache` from the PV.

**How do I change the cache later?**
See [Changing the cache configuration](CACHING_DAEMONSET.md#changing-the-cache-configuration). The mounter DaemonSet only picks up changes when its pods are recreated.

<!-- TODO: add how existing v2 mounts behave during and after the upgrade, and how to downgrade to v2, once the v2 to v3 upgrade path is final. -->
