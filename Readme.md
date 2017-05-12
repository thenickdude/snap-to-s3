# snap-to-s3 (beta)

This tool will turn AWS EBS volume snapshots into temporary EBS volumes, tar them up, 
compress them with LZ4, and upload them to Amazon S3 for you. You can also opt to create an
image of the entire volume by using `dd`, instead of using `tar`.

Once stored on S3, you could add an S3 Lifecycle Rule to the S3 bucket to automatically [migrate
the snapshots into Glacier](http://docs.aws.amazon.com/AmazonS3/latest/dev/lifecycle-transition-general-considerations.html#before-deciding-to-archive-objects).

## Requirements and installation

This tool is only intended to run on Linux, and has only been tested on Ubuntu 16.04.

This tool must be run on an EC2 instance, and can only operate on snapshots within the same
region as the instance.

This is a Node.js application, so if you don't have it installed already, install node and 
npm:

```bash
# Ubuntu
apt-get install nodejs nodejs-legacy npm
```

The "lz4" command-line compression tool will be used to compress the tars, so make sure you 
have it available:

```bash
# Ubuntu
apt-get install liblz4-tool
```

Now you can fetch and install snap-to-s3 from NPM:

```bash
npm install -g snap-to-s3
```

Or if you download this repository, you can install that version instead from the
repository root:

```bash
npm install # Fetch dependencies
npm link    # Add link to this installation to your path
```

Now it'll be on your PATH, so you can run it like so:

```bash
snap-to-s3 --help
```

### Instance metadata service
This tool requires access to the instance metadata service at `http://169.254.169.254:80/`, 
so ensure that your instance does not have a firewall policy that blocks access to this.

### Credentials / IAM policy

This tool needs to create volumes from snapshots, perform uploads to S3, attach and detach 
volumes to/from instances, delete volumes, and add and delete tags.

You can grant these permissions by attaching an IAM Role to your instance with
[the following policy attached](iam-policy.json). Don't forget to update the bucket name in 
that policy with the actual name of the S3 bucket you'll be uploading to. snap-to-s3 will
then be able to use that policy automatically with no further configuration.

If you're not using an IAM Instance Role to give permissions to snap-to-s3, you can provide
an AWS Access Key ID / Secret Access Key pair instead, 
[follow these instructions](http://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-credentials-node.html).

## Disclaimer

This tool works for me and for my use-case, and I'm happy if it works for you too, but you 
might be doing something that I didn't expect. There is a definite potential for data-loss 
if something goes wrong. 

I haven't tested it at all with `LVM` or `mdadm`, and it's likely to fail horribly. Do not 
use. Only use with disks with regular partitions on them (or disks with no partition table, 
i.e. only one normal formatted filesystem).

Note that snapshots will not be deleted for you even after copying them to S3, so you have
the opportunity to verify the snapshot was transferred correctly before removing it
yourself. 

For example, you can use the `--keep-temp-volumes` option to retain the
temporary volume after migration, and run 
`find . -type f -exec md5sum {} \; | sort -k 2 | md5sum` to compute a signature for the 
volume. Then in a different directory, you can download and untar the snapshot you just 
uploaded to S3 (e.g. using the AWS CLI like 
`aws s3 cp s3://backups.example.com/vol-xxx/snap-xxx.tar.lz4 - | lz4 -d | tar -x`), compute
the same signature over the files you unpacked, and ensure the signatures match.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD 
TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN 
NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL 
DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER 
IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN 
CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

## Usage

The easiest way of running snap-to-s3 is:
 
```bash
sudo snap-to-s3 --all --bucket backups.example.com
```

This will search for all snapshots in the current region which have a tag called "snap-to-s3" 
with the value "migrate". Each partition of that volume will be tar'd and compressed with
lz4 before being uploaded to the bucket that you've specified.

The resulting S3 objects will have locations like:

```
s3://backups.example.com/vol-xxx/2017-01-01T00:00:00+00:00 snap-xxx.1.tar.lz4
s3://backups.example.com/vol-xxx/2017-01-01T00:00:00+00:00 snap-xxx.2.tar.lz4
s3://backups.example.com/vol-xxx/2017-05-01T00:00:00+00:00 snap-yyy.tar.lz4
```

Metadata is added to the files on S3 with details of the snapshot that it was created from,
and tags that were applied to the snapshot are copied over (with some substitutions for 
illegal characters).

Here's the full options list:

```
Options

  --all                        Migrate all snapshots whose tag is set to "migrate" ...
  --one                        ... or migrate any one snapshot whose tag is set to "migrate" ...
  --snapshots SnapshotId ...   ... or provide an explicit list of snapshots to migrate (tags are ignored)
  --tag name                   Name of tag you have used to mark snapshots for migration, and to mark
                               created EBS temporary volumes (default: snap-to-s3)

  --bucket name                S3 bucket to upload to (required)
  --mount-point path           Temporary volumes will be mounted here, created if it doesn't already exist
                               (default: /mnt)
  --upload-streams num         Number of simultaneous streams to send to S3 (increases upload speed and
                               memory usage, default: 4)
  --compression-level level    LZ4 compression level (1-9, default: 1)
  --dd                         Use dd to create a raw image of the entire volume, instead of tarring up the
                               files of each partition
  --keep-temp-volumes          Don't delete temporary volumes after we're done with them
  --volume-type type           Volume type to use for temporary EBS volumes (suggest standard or gp2,
                               default: standard)
```

## Resource usage

lz4 consumes the largest portion of the CPU time. If you're on a t2-series instance, you'll 
probably want to use the least amount of compression (`--compression-level 1`), which is the 
default. Faster instances can afford to use up to level 9.

Node will consume the most memory. Of that memory, the S3 upload process will use at least 
`part_size * num_upload_streams` bytes to buffer the upload. 

The part size is set automatically based on the uncompressed size of the data being uploaded, 
it is approximately `uncompressed_size / 9000`, and is always at least 5MB.
 
The number of upload streams defaults to 4. This helps to overcome the upload speed limits 
you would run in to when using a single TCP connection, and reduces the impact of the latency
of starting the upload of the next part. You can change the number of upload streams with the
`--upload-streams` option.

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

If your upload rate is limited by the speed of reifying the snapshot as an EBS volume (you'll
notice this as a high iowait percentage in "top" and low "volume idle" percentages in the EC2
console), you can increase your effective snapshot upload rate by running multiple instances 
of `snap-to-s3` at the same time to upload multiple snapshots in parallel.

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
to S3 are made in at most 9000 parts, which requires 9000 S3 PutObject calls ($0.045 per
volume at $0.005/1000 PUTs).

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

If you end up "poisoning" too many mount-points with this problem, you may need to restart 
your instance in order to clear them.

### Killed process
If Ctrl+C is pressed while an upload is in progress to S3, the multipart-upload to S3 is 
cleanly aborted.

If the process is killed at some other time, the snapshot that was currently being uploaded 
will likely still have its tag set to "migrating", which will prevent it from being migrated 
again when calling `snap-to-s3 --all`. 

You can either manually change that snapshot tag in the EC2 web console to "migrate" 
before trying again, or you can explicitly pass the snapshot id to the `--snapshot` argument 
which will ignore the "migrating" tag for you.

### Incomplete uploads
If something really weird goes wrong, a half-completed multipart upload might be left on S3,
which will continue to incur storage charges. These incomplete uploads do not appear in your
bucket as objects, or in the S3 web console. 

You can use 
[abort-incomplete-multipart](https://github.com/thenickdude/abort-incomplete-multipart) to 
remove those leftovers. Another option is to add a lifecycle policy to the S3 bucket which
automatically deletes incomplete multipart uploads after X days (where X is comfortably longer
than the longest snapshot upload time you expect with snap-to-s3).