# snap-to-s3

This tool will turn AWS EBS volume snapshots into temporary EBS volumes, tar them up, compress 
them with LZ4, and upload them to Amazon S3 for you. You can also opt to create an image of the 
entire volume by using `dd`, instead of using `tar`.

Once stored on S3, you could add an S3 Lifecycle Rule to the S3 bucket to automatically [migrate
the snapshots into Glacier](http://docs.aws.amazon.com/AmazonS3/latest/dev/lifecycle-transition-general-considerations.html#before-deciding-to-archive-objects).

## Requirements and installation

This tool is only intended to run on Linux, and has only been tested on Ubuntu 16.04,
Amazon Linux 2017.03 and Amazon Linux 2 2017.12.

This tool must be run on an EC2 instance, and can only operate on snapshots within the same
region as the instance.

This is a Node.js application, so if you don't have it installed already, install node (at least
version 6.0.0 LTS or newer) and npm:

```bash
# Ubuntu 16.04
curl -sL https://deb.nodesource.com/setup_6.x | sudo -E bash -
sudo apt-get install -y nodejs

# Amazon Linux
curl -sL https://rpm.nodesource.com/setup_6.x | sudo -E bash -
sudo yum install -y nodejs
```

The "lz4" command-line compression tool will be used to compress the tars, so make sure you 
have it available:

```bash
# Ubuntu 16.04
sudo apt-get install liblz4-tool

# Amazon Linux
sudo yum install lz4
# We'll also need git for installation:
sudo yum install git
```

Now you can fetch and install snap-to-s3 from NPM:

```bash
sudo npm install -g snap-to-s3
```

Or if you download `snap-to-s3` from [its GitHub repository](https://github.com/thenickdude/snap-to-s3), 
you can install that version instead from the repository root:

```bash
npm install # Fetch dependencies
npm link    # Link this installation to your $PATH
```

Now it'll be on your $PATH, so you can run it like so:

```bash
sudo snap-to-s3 --help
```

In order to mount and unmount volumes, and read all files for backup, `snap-to-s3` will need
to be run as root or with `sudo`.

### Instance metadata service
This tool requires access to the [instance metadata service](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)
at `http://169.254.169.254:80/`, so ensure that your instance does not have a firewall policy 
that blocks access to this.

### Credentials / IAM policy

This tool needs to create volumes from snapshots, perform uploads to S3, attach and detach 
volumes to/from instances, delete volumes, and add and delete tags. For snapshot validation, it 
also needs to read objects from S3.

You can grant these permissions by attaching an [IAM Role](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html)
to your instance with [the following policy attached](iam-policy.json). Don't forget to update 
the bucket names in that policy with the actual name of the S3 bucket you'll be uploading to. 
`snap-to-s3` will then be able to use that policy automatically with no further configuration.

If you're not using an IAM Instance Role to give permissions to snap-to-s3, you can grant
those permissions to an IAM user and provide an AWS Access Key ID / Secret Access Key pair 
for that user instead, 
[follow these instructions](http://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-credentials-node.html).

## Disclaimer and warnings

This tool works for me and for my use-case, and I'm happy if it works for you too, but you 
might be doing something that I didn't expect. There is a definite potential for data-loss 
if something goes wrong. 

I haven't tested it at all with `LVM` or `mdadm`, and it's likely to fail horribly. Only use 
with disks with regular partitions on them (or disks with no partition table, i.e. only one 
normal formatted filesystem).

It's better that the temporary EBS volumes created from the snapshots don't automatically get 
mounted read-write by your instance's `/etc/fstab`, since this might cause changes to the 
files on the volume during upload. For `dd` backups, this is much more critical.

`snap-to-s3` will tag temporary EBS volumes it creates from snapshots with a tag called
`snap-to-s3` (this is configurable). Accordingly, it will assume that any EBS volume with this
tag is one of its volumes that it can do whatever it likes with (including deleting it).

The fidelity of the backup depends on how well `tar` is able to preserve your files.
`snap-to-s3` calls your system's `tar` using the default options. If you have some unusual 
files (odd extended file attributes, long path names, special characters in filenames) you 
may find that a tar backup isn't perfect. You can use the `--dd` option instead to just image 
the entire volume, but note that this will include data from deleted  files in the "free" 
portion of the drive, and so will increase the backup size considerably for non-full volumes.

Warnings from `tar` will be printed to the screen, but otherwise ignored by `snap-to-s3` 
unless `tar` returns a non-zero exit code. In practice, the only warnings I've seen have come 
from snapshots taken of a running operating system's root disk, where `tar` will note that it
is ignoring unix socket files like `/var/spool/postfix/public/flush` (this is a good thing).

Note that snapshots will not be deleted for you even after copying them to S3, so you have
the opportunity to verify the snapshot was transferred correctly before removing it
yourself. Upload validation can be performed by `snap-to-s3` using the `--validate` option, 
or you could do it yourself manually.

For a manual validation, you could use the `--keep-temp-volumes` option to retain the
temporary volume after migration, and run 
`find . -type f -exec md5sum {} \; | sort -k 2 | md5sum` in that directory to compute a 
signature for the files in the volume. Then in a different directory, you can download and 
untar the snapshot you just uploaded to S3 (e.g. using the [AWS CLI](https://aws.amazon.com/cli/) 
like  `aws s3 cp "s3://backups.example.com/vol-xxx/snap-xxx.tar.lz4" - | lz4 -d | tar -x`), 
compute the same signature over the files you unpacked, and ensure the signatures match.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD 
TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN 
NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL 
DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER 
IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN 
CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

## Usage

### Migrating snapshots to S3
A typical migration command looks like this:
 
```bash
sudo snap-to-s3 --migrate --all --bucket backups.example.com
```

This will search for all snapshots in the current region which have a tag called "snap-to-s3" 
with the value "migrate" (you need to tag these yourself beforehand). The snapshots will be
turned into temporary EBS volumes and attached to your instance. Each partition of those
volumes will be separately tar'd and compressed with lz4 before being uploaded to the bucket 
that you've specified. The temporary EBS volume will then be detached and deleted.
Finally, the snapshot will be tagged with "migrated". 

The resulting S3 objects will have locations like:

```
s3://backups.example.com/vol-xxx/2017-01-01T00:00:00+00:00 snap-xxx.1.tar.lz4
s3://backups.example.com/vol-xxx/2017-01-01T00:00:00+00:00 snap-xxx.2.tar.lz4
s3://backups.example.com/vol-xxx/2017-05-01T00:00:00+00:00 snap-yyy.tar.lz4
```

Metadata is added to the files on S3 with details of the snapshot that it was created from,
and tags that were applied to the snapshot are copied over (with some substitutions for 
illegal characters).

Note that you need to create the bucket beforehand (it's not created for you), and you should
create it in the same region as your snapshots in order to eliminate AWS's inter-region
transfer fees.

### Validating uploaded snapshots
If you want to make sure that the snapshot was uploaded to S3 correctly, you can use the 
"--validate" option. This option can either be added at the same time as you perform your
--migrate:

```bash
sudo snap-to-s3 --migrate --validate --all --bucket backups.example.com
```

Or it can be done in a separate invocation after migration using just the 
"--validate" option. 

```bash
sudo snap-to-s3 --validate --all --bucket backups.example.com
```

If you want to validate it using a separate invocation, you can speed up validation massively 
by providing the "--keep-temp-volumes" option when you perform the --migrate.

The previously-uploaded tar will be downloaded from S3, unpacked, and MD5 
hashes will be computed of all of the files in it. This uses a streaming approach, so 
no extra disk space is needed for temporary files. For dd images, a hash of the entire
raw volume is taken instead.

At the same time, the snapshot being verified will be turned into a temporary EBS volume, 
mounted, and MD5 hashes will be computed of all of its files.

Finally, when both processes are complete, you'll be told if there are any files missing
from the S3 copy of the snapshot, and if any of the file hashes differ. If validation was
successful, the snapshot will be tagged with the value "validated", and the temporary EBS
volume will be detached and deleted.

Note that the tar validation only compares the hashes of the content of regular files. Special 
files like symlinks are not checked at all, and neither are attributes like file permissions.

### Restoring snapshots from S3

`snap-to-s3` doesn't perform snapshot restorations itself, but you can do this with the
[AWS CLI](https://aws.amazon.com/cli/). 

To restore a tar, check the metadata on the archive on S3 to find the 
"x-amz-meta-uncompressed-size" header, this will give you a hint about how large of an EBS 
volume you'll need to create to hold the volume (you'll need somewhat more space than this in 
order to hold filesystem metadata). Create a volume of that size, attach it to the instance, 
create a filesystem on it with `mkfs`, mount it somewhere useful, and enter that directory. Now 
you can download and extract the tar from S3 like so:

```bash
aws s3 cp "s3://backups.example.com/vol-xxx/2017-01-01 snap-xxx.tar.lz4" - | lz4 -d | sudo tar -x
```

If you're restoring an image that was created with dd, create and attach an EBS volume at least 
as large as the "x-amz-meta-snapshot-volumesize" field indicates. If you attached it at
`/dev/xvdf` (for example), then you could restore the snapshot like so:

```bash
aws s3 cp "s3://backups.example.com/vol-xxx/2017-01-01 snap-xxx.img.lz4" - | lz4 -d | sudo dd bs=1M of=/dev/xvdf
```

### Analyzing a Cost and Usage report

`snap-to-s3` can examine an Amazon Cost and Usage report to show you a per-volume and 
per-snapshot breakdown of your EBS snapshot charges, which you can use to identify snapshots 
suitable for migrating to S3/Glacier.

From your Amazon billing dashboard, go to the Reports section, and create a new report. Choose
"daily" for the time period, tick the "Include Resource IDs" box, choose GZip compression,
and select an S3 bucket to store the report in. Around 24 hours later, you should have a .csv.gz 
report in that bucket to analyze. Download it to your instance.

You can pass that file to `snap-to-s3` using any of these styles:

```bash
snap-to-s3 --analyze costreport-1.csv 

snap-to-s3 --analyze costreport-1.csv.gz 

aws s3 cp "s3://cost-reports.example.com/20170501-20170601/x-x-x-x-x/costreport-1.csv.gz" - | snap-to-s3 --analyze
```

`snap-to-s3` will summarize the billing data in the report, then combine it with information about
your current volumes and snapshots (DescribeVolumes and DescribeSnapshots).

Here's an example output. The effective size of each snapshot is shown next to it, this is the
amount of data in the snapshot that differs from the previous snapshot, which is the size you 
are billed for: 

```
Region us-west-2 ($166.16/month for 24 snapshots)
vol-xxx (500GB, MySQL Slave DB): 3016 GB total, $151/month for 16 snapshots, average snapshot change 32%
  snap-xxx  2016-11-01  448.7 GB
  snap-xxx  2016-12-01  261.7 GB (52%)
  snap-xxx  2017-01-01  301.5 GB (60%)
  snap-xxx  2017-02-01  275.4 GB (55%)
  snap-xxx  2017-03-01  250.5 GB (50%)
  snap-xxx  2017-04-01  279.3 GB (56%)
  snap-xxx  2017-05-01  320.6 GB (64%)
  snap-xxx  2017-05-17  218.1 GB (44%)
  snap-xxx  2017-05-18  90.8 GB (18%)
  snap-xxx  2017-05-19  85.2 GB (17%)
  snap-xxx  2017-05-20  89.4 GB (18%)
  snap-xxx  2017-05-21  93.2 GB (19%)
  snap-xxx  2017-05-22  92.6 GB (19%)
  snap-xxx  2017-05-23  82.8 GB (17%)
  snap-xxx  2017-05-24  87.1 GB (17%)
  snap-xxx  2017-05-25  39.5 GB (7.9%)

vol-xxx (20GB, deleted): 5 GB total, $0.247/month for 6 snapshots, average snapshot change 2.1%
  snap-xxx  2015-04-27   2.4 GB
  snap-xxx  2015-05-04   0.1 GB (0.43%)
  snap-xxx  2015-06-01   0.1 GB (0.56%)
  snap-xxx  2015-06-29   0.1 GB (0.62%)
  snap-xxx  2016-04-25   2.2 GB (11%)
  snap-xxx  2016-09-08   0.0 GB (0.069%)
```

In this case, the older snapshots of the first volume change a lot, so the delta encoding scheme
of EBS snapshots isn't saving us very much. These snapshots are a great candidate to move to S3
or Glacier. Whereas the second set of snapshots change by nearly nothing, so S3/Glacier will be
more expensive.

### All options
Here's the full options list:

```
Migrate snapshots to S3

  --migrate                    Migrate EBS snapshots to S3
  --all                        Migrate all snapshots whose tag is set to "migrate"
  --one                        ... or migrate any one snapshot whose tag is set to "migrate"
  --snapshots SnapshotId ...   ... or provide an explicit list of snapshots to migrate (tags are ignored)
  --upload-streams num         Number of simultaneous streams to send to S3 (increases upload speed and
                               memory usage, default: 1)
  --compression-level level    LZ4 compression level (1-9, default: 1)
  --dd                         Use dd to create a raw image of the entire volume, instead of tarring up the
                               files of each partition
  --sse mode                   Enables server-side encryption, valid modes are AES256 and aws:kms
  --sse-kms-key-id id          KMS key ID to use for aws:kms encryption, if not using the S3 master KMS key
  --gpg-recipient keyname      Encrypt the image for the given GPG recipient (add multiple times for
                               multiple recipients)

Validate uploaded snapshots

  --validate                   Validate uploaded snapshots from S3 against the original EBS snapshots (can
                               be combined with --migrate)
  --gpg-session-key key        See readme for details
  --all                        Validate all snapshots whose tag is set to "migrated"
  --one                        ... or validate any one snapshot whose tag is set to "migrated"
  --snapshots SnapshotId ...   ... or provide an explicit list of snapshots to validate (tags are ignored)

Analyze AWS Cost and Usage reports

  --analyze filename   Analyze an AWS Cost and Usage report to find opportunities for savings

Common options

  --help                 Show this page

  --tag name             Name of tag you have used to mark snapshots for migration, and to mark
                         created EBS temporary volumes (default: snap-to-s3)
  --bucket name          S3 bucket to upload to (required)
  --mount-point path     Temporary volumes will be mounted here, created if it doesn't already exist
                         (default: /mnt)
  --keep-temp-volumes    Don't delete temporary volumes after we're done with them
  --volume-type type     Volume type to use for temporary EBS volumes (e.g. standard or gp2), by default
                         standard volumes will be used for volumes 1TiB and smaller, gp2 will be used for
                         larger volumes
```

## Performance

The snapshot migration rate that `snap-to-s3` achieves seems to be largely limited by how fast
EC2 will turn a snapshot into a completely-readable volume (i.e. the rate they can copy blocks
from their own private S3 snapshot storage into EBS). For me a simple `dd` from an EBS volume 
(freshly created from a snapshot) to `/dev/zero` averages a transfer rate of 3.5MiB/s, which 
gives an expected 24 hours to upload a 300GiB snapshot with `snap-to-s3`.

If your migration rate is being limited by this, you'll notice it as a high iowait percentage 
in "top" and low "volume idle" percentages in the EC2 console. In this situation you can 
increase your effective snapshot upload rate by running multiple instances of `snap-to-s3` at 
the same time to upload multiple snapshots in parallel. This allows you to scale your upload 
rate nearly linearly with the number of snapshots being uploaded, until other limits are 
reached like network speed and CPU usage.

## Resource usage

lz4 consumes the largest portion of the CPU time. If you're on a t2-series instance, you'll 
probably want to use the least amount of compression (`--compression-level 1`), which is the 
default. Faster instances can afford to use up to level 9.

Node will consume the most memory. Of that memory, the S3 upload process will use at least 
`part_size * num_upload_streams` bytes to buffer the upload. 

The part size is set automatically based on the uncompressed size of the data being uploaded, 
it is approximately `uncompressed_size / 9000`, and is always at least 5MB.
 
The number of upload streams defaults to 1. If your snapshot volume can be read very quickly
(e.g. if you are using EBS Snapshot Fast Restore), this may be a bottleneck, and you may
want to bump this up using the `--upload-streams` option.

Here's the minimum amount of memory that would be consumed with various volume sizes (for 
100% full volumes):

| Volume size | Memory with streams = 1 | Memory with streams = 4 |
| ----------- | ----------------------- | ----------------------- |
| 1GB         | 5MB                     | 20MB                    |
| 40GB        | 5MB                     | 20MB                    |
| 100GB       | 11MB                    | 46MB                    |
| 200GB       | 23MB                    | 91MB                    |
| 400GB       | 46MB                    | 180MB                   |
| 800GB       | 91MB                    | 360MB                   |
| 1600GB      | 180MB                   | 730MB                   |
| 3200GB      | 360MB                   | 1500MB                  |

On top of this, inefficiencies in Node's memory management (especially the garbage collector) 
will likely require a factor more memory, and there's a fixed-size overhead of around 100MB.
Test it out with your snapshots/Node version if memory is tight.

## Cost analysis

### Storage costs

At the time of writing and in the region I use, EBS snapshots were charged at $0.05/GB-month, 
S3 Standard at $0.023/GB-month, S3 Infrequent-Access at $0.0125/GB-month, and Glacier at 
$0.004/GB-month, so migrating snapshots to S3 or Glacier could potentially save you on your 
monthly storage bill.

However, keep in mind that EBS snapshots are incremental; if you have two snapshots of the
same volume, the second snapshot will only require enough storage space to hold the blocks 
that changed since the previous snapshot. In contrast, snapshots pushed to S3 or Glacier with 
this tool are full backups, not incremental.

This means that the cost difference between EBS snapshots and S3/Glacier will depend on how 
much your successive snapshots differ.

If you have many snapshots of the same volume (i.e., in the limit as the number of snapshots
reaches infinity), and your volume changes by more than 46% between successive snapshots, S3 
Standard is cheaper than EBS snapshots. For S3 Infrequent Access, the breakeven point is at
25%.

For Glacier, the breakeven point comes much sooner, with volumes changing more than 8% between 
snaps being cheaper to store on Glacier. 

If LZ4 achieves a 2:1 compression ratio on your data, this breakeven point is correspondingly
halved (i.e. Glacier would break-even at 4% change between snapshots).

For a smaller number of snapshots, the breakeven point is reached earlier. Let's say 
that you are slowly outgrowing your volume sizes, so you create a new volume each year, and 
you want to retain one snapshot per month (12 snapshots for the lifetime of the volume). 
Here's the price of S3 and Glacier as a percentage of the cost of EBS snapshots.

| Change between snapshots | S3 Std  | S3 IA | Glacier |
| ------------------------ | ------- | ----- | ------- |
| 0%                       | 550%    | 300%  | 96%     |
| 1%                       | 500%    | 270%  | 86%     |
| 2%                       | 450%    | 250%  | 79%     |
| 3%                       | 420%    | 230%  | 72%     |
| 4%                       | 380%    | 210%  | 67%     |
| 5%                       | 360%    | 190%  | 62%     |
| 10%                      | 260%    | 140%  | 46%     |
| 15%                      | 210%    | 110%  | 36%     |
| 20%                      | 170%    | 94%   | 30%     |
| 30%                      | 130%    | 70%   | 22%     |
| 40%                      | 102%    | 56%   | 18%     |
| 50%                      | 85%     | 46%   | 15%     |
| 75%                      | 60%     | 32%   | 10%     |
| 100%                     | 46%     | 25%   | 8.0%    |

Notice that if you only have 12 snapshots of a given volume, Glacier is always cheaper than
EBS snapshots, no matter how much each snapshot changes compared to the previous one.

Here's the same situation if you achieve a 2:1 compression ratio using LZ4:

| Change between snapshots | S3 Std 2:1 | S3 IA 2:1 | Glacier 2:1 |
| ------------------------ | ---------- | --------- | ----------- |
| 0%                       | 280%       | 150%      | 48%         |
| 1%                       | 250%       | 140%      | 43%         |
| 2%                       | 230%       | 120%      | 39%         |
| 3%                       | 210%       | 110%      | 36%         |
| 4%                       | 190%       | 104%      | 33%         |
| 5%                       | 180%       | 97%       | 31%         |
| 10%                      | 130%       | 71%       | 23%         |
| 15%                      | 104%       | 57%       | 18%         |
| 20%                      | 86%        | 47%       | 15%         |
| 30%                      | 64%        | 35%       | 11%         |
| 40%                      | 51%        | 28%       | 8.9%        |
| 50%                      | 42%        | 23%       | 7.4%        |
| 75%                      | 30%        | 16%       | 5.2%        |
| 100%                     | 23%        | 12.5%     | 4.0%        |

If you want to play with the parameters that generated this table you can do so using [this
spreadsheet on Google Docs](https://docs.google.com/spreadsheets/d/1NH0XS5_HSuHJ8K5WOgSPYRFVW0OKFR1j1NgPLi9ig7E/edit?usp=sharing).
You'll need to download it or copy it to your own account to be able to edit the fields.

S3 Infrequent Access archives have their storage costs charged based on a minimum 30-day
lifetime, even if you delete them or migrate them to Glacier before then.

Glacier archives have their storage costs charged based on a minimum 90-day lifetime, even if 
you delete them sooner.

Don't forget that restoring from Glacier takes longer and costs much more than from EBS or S3. 
It's mostly suitable for archival backups. 

### Migration costs

There are several costs involved in using `snap-to-s3`. Here are some of the costs that I 
consider significant in my use-case. Your use-case may vary:

Pushing snapshots to S3 will require the use of an EC2 instance for some hours (prices vary,
especially depending on how many snapshots you upload at once). You'll pay EBS storage and I/O 
costs for the temporary volumes created from the snapshots while they are uploading. Uploads 
to S3 are made in at most 9000 parts, which requires 9000 S3 PutObject calls (so $0.045 per
volume if charged at $0.005/1000 PUTs).

Avoid uploading to a S3 bucket in a different region, since it will incur inter-region transfer
costs for both upload and download.

There are other per-request costs that should only become significant if you are migrating
thousands of snapshots. Please read the relevant Amazon documentation for details.

## If something goes wrong
### Stuck volumes
Occasionally, EC2 fails to properly attach a volume to the instance, and it gets stuck in the
"attaching" state. You'll get this error message on the command line:

```
[snap-xxx] An error occurred, tagging snapshot with "migrate" so it can be retried later
[snap-xxx] Timed out waiting for vol-xxx to appear in /dev
```

On the EC2 web console, use the "force detach" option on the volume, then reattach it on a 
different mount-point and re-run snap-to-s3. 

If you end up "poisoning" too many mount-points with this problem, you may need to stop and
start your instance in order to clear them.

### Killed process
If Ctrl+C is pressed while an upload is in progress to S3 (sending a SIGINT), the 
multipart-upload to S3 is cleanly aborted.

If the process gets SIGINT at some other time, the snapshot that was currently being uploaded 
will likely still have its tag set to "migrating", which will prevent it from being migrated 
again when calling `snap-to-s3 --migrate --all`. 

You can either manually change that snapshot tag in the EC2 web console to "migrate" 
before trying again, or you can explicitly pass the snapshot id to the `--snapshots` argument 
which will ignore the "migrating" tag for you.

If `snap-to-s3` receives a `SIGHUP` from your SSH session dropping, it will be killed. 
Consider running snap-to-s3 in a `screen`/`tmux` session, or with 
[nohup](https://en.wikipedia.org/wiki/Nohup).

### Incomplete uploads
If something really weird goes wrong, (e.g. process receives SIGKILL due to out of memory
condition) a half-completed multipart upload might be left on S3, which will continue to 
incur storage charges. These incomplete uploads do not appear in your bucket as objects, or 
in the S3 web console. 

You can use 
[abort-incomplete-multipart](https://github.com/thenickdude/abort-incomplete-multipart) to 
remove those leftovers. Another option is to add a lifecycle policy to the S3 bucket which
automatically deletes incomplete multipart uploads after X days (where X is comfortably longer
than the longest snapshot upload time you expect with snap-to-s3).

## Encrypting snapshots with GPG

Snapshots can be additionally encrypted using `gpg2`'s asymmetric encryption before they are uploaded to S3. By allowing
the corresponding private key to be stored offsite (perhaps in a hardware device such as a YubiKey), this allows 
encrypted snapshots to be stored on S3 that cannot be decrypted even if the AWS root account is compromised.

You need to have gpg2 installed (typically provided by the `gnupg2` package), and the public keys that you will encrypt
the snapshot to need to be loaded into GPG and marked as trusted (e.g. with `gpg2 --import` or `gpg2 --recv-keys`, 
then `gpg2 --edit-key`). Then when you call `snap-to-s3 --migrate`, you can add `--gpg-recipient` arguments (one per 
public key) that name the public keys that GPG will encrypt the snapshot to, for example:

```bash
snap-to-s3 --migrate --gpg-recipient SnapToS3Example --bucket backups.example.com --snapshots snap-xxx
```

However, the encryption poses a challenge if you want to use the `--validate` command, because the snapshot needs to be 
decrypted in order to perform validation, and this requires the corresponding private key to be available to `snap-to-s3`.

There are two main strategies for dealing with this:

### Validation where the private key is available to snap-to-s3

Let's imagine that you were okay with storing both the public and private keys for the snapshot on the `snap-to-s3` 
instance (after carefully considering the security implications). If you don't already have one, you could generate 
such a key like this:

```
# gpg2 --full-gen-key

gpg (GnuPG) 2.1.11; Copyright (C) 2016 Free Software Foundation, Inc.

Please select what kind of key you want: 1 RSA and RSA (default)
What keysize do you want? (2048) 2048
Key is valid for? (0) 10y

Real name: SnapToS3Example
Email address: n.sherlock@gmail.com

You selected this USER-ID:
    "SnapToS3Example <n.sherlock@gmail.com>"
    
gpg: Please enter a passphrase to protect your private key
...

gpg: key 142906AF marked as ultimately trusted

gpg: checking the trustdb
gpg: marginals needed: 3  completes needed: 1  trust model: PGP
gpg: depth: 0  valid:   3  signed:   0  trust: 0-, 0q, 0n, 0m, 0f, 3u
gpg: next trustdb check due at 2029-01-18
pub   rsa2048/142906AF 2019-01-21 [S] [expires: 2029-01-18]
      Key fingerprint = 06C9 ADB1 E792 4C6F 5CF0  5473 98FC 45D4 1429 06AF
uid         [ultimate] SnapToS3Example <n.sherlock@gmail.com>
sub   rsa2048/84B62A1E 2019-01-21 [] [expires: 2029-01-18]
```

Then you can use snap-to-s3 to encrypt a snapshot to that key like so:

```bash
snap-to-s3 --migrate --validate --gpg-recipient SnapToS3Example --bucket backups.example.com --snapshots snap-xxx
```

However, in order for the `--validate` to succeed, the private key needs to have its passphrase unlocked. `snap-to-s3` 
is not able to accept keyboard input while uploading, so gpg will fail when it tries to prompt for a passphrase.
You can make this work by "presetting" your passphrase into the GPG agent instead (using the `gnupg-agent` package).

Passphrase presetting is disabled in the agent's default configuration. Edit `~/.gnupg/gpg-agent.conf` (you'll probably 
need to create this file) to add:

```
allow-preset-passphrase
```

Restart the agent to make it reload this config by running:

```bash
gpg-connect-agent reloadagent /bye
```

To preset the passphrase for a key, we must first find out the key's "keygrip". To do this run:

```bash
# gpg2 --list-keys --with-keygrip

/root/.gnupg/pubring.kbx
------------------------
pub   rsa2048/142906AF 2019-01-21 [SC] [expires: 2029-01-18]
      Keygrip = 22C54011D7772B7B23F2FBD0D69AAE74873BEAD0
uid         [ultimate] SnapToS3Example <n.sherlock@gmail.com>
sub   rsa2048/84B62A1E 2019-01-21 [E] [expires: 2029-01-18]
      Keygrip = 04A78677528EE326B8AFEE81C4823B47F30DECDC
```

The keygrip we need is the one for the "[E]" (encryption) subkey. Now use that keygrip to run:

```bash
/usr/lib/gnupg2/gpg-preset-passphrase --preset 04A78677528EE326B8AFEE81C4823B47F30DECDC
``` 

The utility will then wait for you to enter your passphrase and press enter. Note that your passphrase will be echoed 
out to the terminal in the clear (this utility doesn't replace it with stars to hide it).

Now `snap-to-s3` should be able to successfully validate the upload of snapshots that use this key (until the system or 
GPG agent is restarted).

### Validation where the private key never leaves your local machine

You may not want your private key to ever touch Amazon, or to ever be exported from your local hardware key storage 
device. This means that `snap-to-s3` will not be able to use that key to decrypt its uploaded archives for validation, 
so `--validate` will fail.

One way of solving this is to generate an additional keypair just for `snap-to-s3` to use. Cache the passphrase for
snap-to-s3's key using the instructions in the previous section. When migrating your snapshots,
add both your original keypair and the `snap-to-s3` keypair as recipients with `--gpg-recipient` (e.g. 
`--gpg-recipient SnapToS3Example --gpg-recipient "Nicholas Sherlock"`). Then once you're 
satisfied that your snapshots have uploaded correctly using `--validate`, you can securely destroy the snap-to-s3 
keypair, leaving your original keypair as the only surviving key that can decrypt the snapshots. If you're not keen on 
that solution, read on: 

When GPG encrypts an archive, it generates a random session key, which is used to encrypt the archive using symmetric
encryption, then that session key is encrypted with the public key of the recipient and stored into the archive. 
For decryption, the process is reversed: first the session key is decrypted with the corresponding private key. That's
the part that `snap-to-s3` cannot perform, since it doesn't have access to the private key.

However, you can do that part of the process yourself manually on your local machine. First, use `snap-to-s3` to encrypt
and upload your snapshot using `--migrate --keep-temp-volumes --gpg-recipient YourPublicKey` (no `--validate`). 
Then, on your local machine where your private key resides, you can decrypt the session key from the uploaded file like 
so:

```bash 
aws s3 cp "s3://backups.example.com/vol-xxx/2019-01-21T00:16:59+00:00 snap-xxx.tar.lz4.gpg" - \
	| head -c 524288 \
	| gpg2 --decrypt --show-session-key \
	> /dev/null
	
gpg: encrypted with 4096-bit RSA key, ID 45BE6A42B05996C3, created 2018-08-08
      "Nicholas Sherlock <n.sherlock@gmail.com>"
gpg: session key: '9:D41D8DB13C9CA64F9E24C697973599B6B1E71BEFE8C7BAB41AC8FD97C4F14143'
gpg: block_filter 0x00007f8fc1100020: read error (size=11277,a->size=11277)
gpg: WARNING: encrypted message has been manipulated!
gpg: block_filter: pending bytes!
```

This command fetches the first 512kB of the archive, which is large enough that it should contain the encrypted session 
key packet (since that appears at the start of the archive), then pipes that to GPG to decrypt and print out the session 
key. GPG will prompt you for your passphrase (or your hardware key device) for decryption. (The decrypted archive output 
that would normally be sent to stdout is piped to `/dev/null` to be discarded.) You can ignore the error messages, as 
they are merely being triggered by the file being truncated by `head`. 

Back on your EC2 instance, you can now give that key to `snap-to-s3` for validation:

```bash
snap-to-s3 --validate --bucket backups.example.com --snapshots snap-xxx --gpg-session-key "9:D41D8DB13C9CA64F9E24C697973599B6B1E71BEFE8C7BAB41AC8FD97C4F14143"
```

This key allows `snap-to-s3` to decrypt the archive for verification, without having to give it access to your private 
GPG key.

Note that because you can only supply a single session key to `snap-to-s3`, you can only validate a single snapshot at
a time using this method.