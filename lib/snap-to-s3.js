"use strict";

const
	fs = require("fs"),
	child_process = require("child_process"),
	querystring = require("querystring"),
	assert = require("assert"),
	which = require("which"),
	crypto = require("crypto"),
	util = require("util"),
	EventEmitter = require("events").EventEmitter,
	
	clone = require("clone"),
	Logger = require("js-logger"),
	moment = require("moment"),
	filesize = require("filesize"),
	ProgressBar = require("progress"),
	AWS = require("aws-sdk"),
	
	awsTools = require("./aws-tools"),
	fsTools = require("./filesystem-tools"),
	SimpleProgressStream = require("./simple-progress-stream"),
	
	metadataService = new AWS.MetadataService();

const
	TAG_RACE_DELAY = 4000,
	PROGRESS_BAR_UPDATE_RATE = 5000, // ms
	PARTITION_POLL_INTERVAL = 4 * 1000, // ms
	PARTITION_POLL_MAX_RETRY = 75;

function fetchInstanceDetails() {
	return new Promise(function(resolve, reject) {
		metadataService.request("/latest/dynamic/instance-identity/document", function (err, instanceIdentity) {
			if (err) {
				reject(err);
			} else {
				instanceIdentity = JSON.parse(instanceIdentity);
				
				AWS.config.update({
					region: instanceIdentity.region
				});
				
				resolve(instanceIdentity);
			}
		});
	});
}

/**
 * Get a description of all of the listed snapshots, fail if any could not be found.
 *
 * @param ec2
 * @param {String[]} snapshotIDs
 * @returns {Promise.<EC2.SnapshotList>}
 */
function describeSnapshots(ec2, snapshotIDs) {
	return ec2.describeSnapshots({
		SnapshotIds: snapshotIDs
	}).promise().then(
		data => {
			let
				accountedFor = {},
				missing;
			
			for (let snapshot of data.Snapshots) {
				accountedFor[snapshot.SnapshotId] = true;
			}
			
			missing = snapshotIDs.filter(snapshotID => !(snapshotID in accountedFor));
			
			if (missing.length > 0) {
				throw new SnapshotsMissingError(missing);
			} else {
				return data.Snapshots;
			}
		}
		/*
		 * If describeSnapshots returns an error, we can't turn the error into a SnapshotsMissingError, because we
		 * can't get the affected snapshot IDs out of the exceptions AWS throws (InvalidSnapshot.NotFound,
		 * InvalidSnapshotID.Malformed). So just pass AWS's error through instead.
		 */
	);
}

function createS3KeyForSnapshotPartitionTar(snapshot, partitionName) {
	let
		filename = snapshot.VolumeId + "/" + moment(snapshot.StartTime).format() + " " + snapshot.SnapshotId;
		
	if (snapshot.Description.length > 0) {
		filename += " - " + snapshot.Description;
	}
	
	if (partitionName.length > 0) {
		filename += "." + partitionName;
	}
	
	return filename + ".tar.lz4";
}

function createS3KeyForSnapshotVolumeImage(snapshot) {
	let
		filename = snapshot.VolumeId + "/" + moment(snapshot.StartTime).format() + " " + snapshot.SnapshotId;
	
	if (snapshot.Description.length > 0) {
		filename += " - " + snapshot.Description;
	}

	return filename + ".img.lz4";
}

/**
 * Replace special characters in a tag value with underscores:
 *
 * http://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html
 *
 * @param {string} value
 * @returns {string}
 */
function sanitiseS3TagValue(value) {
	return value.replace(/[^a-zA-Z0-9+=._:/\s-]/g, "_");
}

function checkForRequiredBinaries() {
	let
		binaries = ["lsblk", "lz4", "tar", "du", "mount", "umount"];
	
	for (let binary of binaries) {
		try {
			which.sync(binary);
		} catch (e) {
			throw "Missing required utility '" + binary + "', is it on the path?";
		}
	}
}

/**
 * Filters a list of block devices from one volume to give a list of devices that could contain filesystems.
 *
 * @param {BlockDevice[]} devices
 * @returns {BlockDevice[]}
 */
function filterBlockDevicesToGetFilesystems(devices) {
	/* If a disk has no partition table, there'll be one block device of type "disk", and we should return this.
	 *
	 * Otherwise it'll have one "disk" device to represent the whole raw volume, which we don't want, and a series of
	 * "parts" that should contain filesystems, which we'll return.
	 */
	if (devices.length > 1) {
		const
			expectedPartitionCount = devices.length - 1; // We only expect to remove one "disk" device
		
		// Remove the single disk entry:
		devices = devices.filter(device => device.TYPE !== "disk");
		
		// Make sure there really was only one disk:
		assert(devices.length === expectedPartitionCount);
		
		// And make sure there are no bogon block devices mixed in:
		devices = devices.filter(partition => partition.TYPE === "part");
		
		if (devices.length !== expectedPartitionCount) {
			throw "This volume contains block devices of an unknown type (neither 'disk' nor 'part')";
		}
	}
	
	if (devices.length === 0) {
		throw "Couldn't find any partitions in this volume!";
	}
	
	return devices;
}

/**
 * Filters a list of block devices from one volume to just get the device that represents the whole raw disk.
 *
 * @param {BlockDevice[]} devices
 * @returns {BlockDevice}
 */
function filterBlockDevicesToGetRawDisk(devices) {
	devices = devices.filter(device => device.TYPE === "disk");
	
	assert(devices.length === 1);
	
	return devices[0];
}

/**
 * Find and transfer snapshots to S3.
 *
 * @param {Object} options
 */
function SnapToS3(options) {
	EventEmitter.call(this);
	
	this.setOptions(options);
	
	this.initPromise =
		fsTools.forcePath(this.options["mount-point"])
			.then(checkForRequiredBinaries)
			.then(fetchInstanceDetails)
			.then(instanceIdentity => {
				this.instanceIdentity = instanceIdentity;
				
				// Now AWS has been configured with our region, we can create these:
				this.s3 = new AWS.S3({
					signatureVersion: 'v4' // Support aws:kms server-side encryption
				});
				this.ec2 = new AWS.EC2();
			});
}

util.inherits(SnapToS3, EventEmitter);

SnapToS3.prototype.setOptions = function(options) {
	const
		optionDefaults = {
			"volume-type": "standard",
			"compression-level": 1,
			"upload-streams": 4,
			"keep-temp-volumes": false,
			"dd": false
		},
		// Options with no defaults that we require the caller to supply:
		requiredOptions = ["tag", "mount-point", "bucket"];
	
	this.options = Object.assign({}, options);
	
	for (let defaultOption in optionDefaults) {
		if (this.options[defaultOption] === undefined) {
			this.options[defaultOption] = optionDefaults[defaultOption];
		}
	}
	
	for (let requiredOption of requiredOptions.concat(Object.keys(optionDefaults))) {
		if (this.options[requiredOption] === undefined || (typeof this.options[requiredOption] === "string" && this.options[requiredOption].length === 0)) {
			throw "Missing required option '" + requiredOption + "'";
		}
	}
	
	// Force mount-point to end in a slash
	if (!this.options["mount-point"].match(/\/$/)) {
		this.options["mount-point"] = this.options["mount-point"] + "/";
	}
	
	if (this.options["mount-point"] === "/") {
		throw "Mount point must not be empty, or /";
	}
	
	this.options["compression-level"] = Math.min(Math.max(Math.round(this.options["compression-level"]), 1), 9);
	this.options["upload-streams"] = Math.max(Math.round(this.options["upload-streams"]), 1);
	
	if (this.options["sse-kms-key-id"] !== undefined && this.options.sse !== "aws:kms") {
		throw "You specified an SSE KMS Key ID, but you didn't set --sse to \"aws:kms\"";
	}
};


/**
 * Compress the stdout of the given process using lz4 and upload it to s3.
 *
 * @param sourceProcess - Process to upload the stdout of
 * @param {int} streamLengthEstimate - Size of stream in bytes
 * @param {S3.TagList} tags
 * @param {S3.PutObjectRequest} _s3Params - S3 params object to pass to the S3 upload class
 * @param {ILogger} logger
 *
 * @returns {Promise}
 */
SnapToS3.prototype.uploadProcessStdOut = function(sourceProcess, streamLengthEstimate, tags, _s3Params, logger) {
	return new Promise((resolve, reject) => {
		const
			lz4 = child_process.spawn("lz4", ["-z", "-" + this.options["compression-level"]], {
				stdio: ["pipe", "pipe", "pipe"]
			}),
			
			progressStream = new SimpleProgressStream(),
			
			bar = new ProgressBar('  uploading [:bar] :rate KB/s :percent :eta', {
				complete: '=',
				incomplete: ' ',
				width: 32,
				total: Math.floor(streamLengthEstimate / 1024),
				renderThrottle: PROGRESS_BAR_UPDATE_RATE
			}),
			
			/**
			 * Don't modify the caller's s3Params, they might want to re-use it
			 *
			 * @type {S3.PutObjectRequest}
			 */
			s3Params = clone(_s3Params);
		
		let
			failed = false,
			upload,
			progressBytesTotal = 0,
			
			_reject = (err) => {
				reject(err);
				
				if (!failed) {
					failed = true;
					
					if (upload) {
						upload.abort();
					}
				}
			},
			
			ctrlCHandler = () => {
				logger.error("SIGINT received, aborting upload...");
				upload.abort();
			};
		
		logger.info(filesize(streamLengthEstimate) + " to compress and upload to s3://" + s3Params.Bucket + "/" + s3Params.Key);
		logger.info("Progress is based on the pre-compression data size:");
		
		sourceProcess.on("error", _reject);
		lz4.on("error", _reject);
		
		sourceProcess.on("close", (code) => {
			if (code !== 0 && !failed) {
				_reject("Reading failed! Return code " + code);
			}
		});
		lz4.on("close", (code) => {
			if (code !== 0 && !failed) {
				_reject("lz4 failed! Return code " + code);
			}
		});
		
		sourceProcess.stderr.on("data", function (data) {
			// If the upload is already failed then we don't need to report further errors from our subprocesses:
			if (!failed) {
				process.stderr.write(data);
			}
		});
		lz4.stderr.on("data", function (data) {
			if (!failed) {
				process.stderr.write(data);
			}
		});
		
		// The streams trigger an error event upon forced .end() (upload abort) that we must handle:
		for (let stream of [sourceProcess.stdout, sourceProcess.stderr, progressStream, lz4.stdin, lz4.stdout, lz4.stderr]) {
			stream.on("error", _reject);
		}
		
		// Connect the "sourceProcess -> progress -> lz4 ->" pipeline together:
		sourceProcess.stdout.pipe(progressStream).pipe(lz4.stdin);
		
		// And the output of that pipeline is what we'll upload:
		s3Params.Body = lz4.stdout;
		
		// So we'll know approximately what size of volume we can provision to restore this snapshot later
		s3Params.Metadata["uncompressed-size"] = streamLengthEstimate + "";
		
		// We don't want to resume an upload!
		assert(s3Params.UploadId === undefined);
		
		// This is the default, but let's make sure that AWS computes MD5 sums of each part for us
		AWS.config.update({
			computeChecksums: true
		});
		
		upload = new AWS.S3.ManagedUpload({
			params: s3Params,
			leavePartsOnError: false,
			queueSize: this.options["upload-streams"],
			partSize: Math.max(
				/* Leave some fudge factor for the tar overhead, and just in case the volume becomes larger under compression: */
				Math.ceil((streamLengthEstimate + 10 * 1024 * 1024) / (AWS.S3.ManagedUpload.prototype.maxTotalParts * 0.9)),
				AWS.S3.ManagedUpload.prototype.minPartSize
			),
			tags: tags, // The uploader can construct the Tagging header for us
			service: this.s3
		});
		
		progressStream.on("progress", bytesRead => {
			progressBytesTotal += bytesRead;
			
			// We'll need to refine our crude total size estimate towards the end of the upload:
			streamLengthEstimate = Math.max(progressBytesTotal, streamLengthEstimate);
			
			bar.total = Math.floor(streamLengthEstimate / 1024);
			
			if (!failed) {
				// Avoid telling the bar to print out "100%" multiple times (it causes the bar to duplicate itself)
				if (!bar.complete) {
					bar.update(progressBytesTotal / streamLengthEstimate);
				}
			}
		});
		
		bar.render(null);
		
		upload.promise().then(
			() => {
				process.removeListener("SIGINT", ctrlCHandler);
				
				// Do one last update to finish the bar
				if (!bar.complete) {
					bar.update(1.0);
				}
				
				// Double check just in case sourceProcess/lz4 reported a failure after our upload completed: (possible?)
				if (!failed) {
					resolve();
				} else {
					// Otherwise we've already reported our reject(). We could delete the S3 file if this happens.
				}
			},
			(err) => {
				process.removeListener("SIGINT", ctrlCHandler);
				
				bar.terminate();
				
				reject("S3 upload failed: " + err);
				failed = true;
				
				try {
					sourceProcess.stdout.unpipe(progressStream);
					progressStream.unpipe(lz4.stdin);
					
					lz4.stdout.end();
					lz4.stdin.end();
					progressStream.end();
					sourceProcess.stdout.end();
				} catch (e) {
					logger.error(e);
				}
			}
		);
		
		/* Handle SIGINT so that CTRL+C cancels the upload rather than killing our process. This prevents incomplete
		 * multipart uploads from sitting around costing money on S3.
		 */
		process.on('SIGINT', ctrlCHandler);
	});
};

/**
 * S3 is rather more restrictive in character set compared to EBS/EC2 (for example, parentheses are not
 * allowed) so we sanitise the key/value first.
 *
 * @param {AWS.TagList} tags
 */
SnapToS3.prototype.sanitiseSnapshotTagsForS3 = function(tags) {
	let
		result = [];
	
	for (let tag of tags) {
		// Don't include the tags we added to the snapshot ourselves
		if (tag.Key !== this.options.tag && tag.Key !== this.options.tag + "-id") {
			result.push({
				Key: sanitiseS3TagValue(tag.Key),
				Value: sanitiseS3TagValue(tag.Value)
			});
		}
	}
	
	return result;
};

/**
 * Find snapshots that are completed, and are tagged with the given value.
 *
 * @returns {Promise.<EC2.SnapshotList>}
 */
SnapToS3.prototype.findCompletedSnapshotsWithTag = function(tagValue) {
	return this.ec2.describeSnapshots({
		Filters: [
			{
				Name: "status",
				Values: [
					"completed"
				]
			},
			{
				Name: "tag:" + this.options.tag,
				Values: [
					tagValue
				]
			}
		],
		OwnerIds: [
			this.instanceIdentity.accountId
		]
	}).promise().then(data => data.Snapshots);
};


/**
 * Find snapshots that are ready for migration.
 *
 * @returns {Promise.<EC2.SnapshotList>}
 */
SnapToS3.prototype.findMigratableSnapshots = function() {
	return this.findCompletedSnapshotsWithTag("migrate");
};

/**
 * Find snapshots that are ready for validation.
 *
 * @returns {Promise.<EC2.SnapshotList>}
 */
SnapToS3.prototype.findValidatableSnapshots = function() {
	return this.findCompletedSnapshotsWithTag("migrated");
};

/**
 * Find a volume which we've tagged as a temporary copy of the given snapshot, ready for copying to S3.
 *
 * @param {EC2.Snapshot} snapshot
 * @returns {Promise.<EC2.Volume>}
 */
SnapToS3.prototype.findTemporaryVolumeForSnapshot = function(snapshot) {
	return this.ec2.describeVolumes({
		Filters: [
			{
				Name: "tag-key",
				Values: [
					this.options.tag
				]
			},
			{
				Name: "snapshot-id",
				Values: [
					snapshot.SnapshotId
				]
			},
			{
				Name: "availability-zone",
				Values: [
					this.instanceIdentity.availabilityZone
				]
			}
		]
	}).promise().then(data => {
		for (/** @type EC2.Volume */let volume of data.Volumes) {
			// Make doubly sure so we don't try to steal the wrong volume...
			assert(volume.SnapshotId === snapshot.SnapshotId);
			assert(volume.Tags.find(tag => tag.Key === this.options.tag));
			assert(volume.AvailabilityZone === this.instanceIdentity.availabilityZone);
			
			// Don't consider using volumes that are attached to other instances
			let
				inUseBySomeoneElse = volume.Attachments.find(attachment => attachment.InstanceId !== this.instanceIdentity.instanceId);
			
			if (!inUseBySomeoneElse) {
				return volume;
			}
		}
		
		return null;
	});
};

/**
 * Create a temporary EBS volume from the given snapshot.
 *
 * @param {String} snapshotID
 *
 * @returns {Promise.<EC2.Volume>}
 */
SnapToS3.prototype.createTemporaryVolumeFromSnapshot = function(snapshotID) {
	return this.ec2.createVolume({
		SnapshotId: snapshotID,
		AvailabilityZone: this.instanceIdentity.availabilityZone,
		VolumeType: this.options["volume-type"],
		TagSpecifications: [
			{
				ResourceType: "volume",
				Tags: [
					{
						Key: "Name",
						Value: "Temp for snap-to-s3"
					},
					{
						Key: this.options.tag,
						Value: 'in-progress'
					}
				]
			}
		]
	}).promise();
};

/**
 * Mark the snapshot with the given tag value. If other processes attempted to do the same thing *at a similar time*,
 * this promise will only succeed if we won that race.
 *
 * @param {String} snapshotID
 * @param {String} tagValue value to set
 * @returns {Promise}
 */
SnapToS3.prototype.raceToMarkSnapshot = function(snapshotID, tagValue) {
	let
		randomID = crypto.randomBytes(4).readUInt32LE(0) + "";
	
	return this.ec2.createTags({
		Resources: [
			snapshotID
		],
		Tags: [
			{
				Key: this.options.tag,
				Value: tagValue
			},
			{
				Key: this.options.tag + "-id",
				Value: randomID
			}
		]
	}).promise().then(() => new Promise((resolve, reject) => {
		/*
		 * Another process might have spotted the unclaimed snapshot and called createTags at the same
		 * time as us! Wait a bit to allow the state to settle before we see if we won the race or not.
		 */
		setTimeout(() => {
			this.ec2.describeTags({
				Filters: [{
					Name: "resource-id",
					Values: [
						snapshotID
					]
				}]
			}).promise().then(
				data => {
					let
						marked = false,
						wonTheRace = false;
					
					for (let tag of data.Tags) {
						switch (tag.Key) {
							case this.options.tag:
								marked = tag.Value === tagValue;
								break;
							case this.options.tag + "-id":
								wonTheRace = tag.Value === randomID;
								break;
						}
					}
					
					if (marked && wonTheRace) {
						resolve(snapshotID);
					} else if (marked) {
						reject("Another process has already marked " + snapshotID + " as " + tagValue);
					} else {
						reject("Tried to mark " + snapshotID + " as " + tagValue + " but another process marked it with a different value");
					}
				},
				err => reject
			);
		}, TAG_RACE_DELAY);
	}));
};

/**
 * Mark the snapshot with the given tag and clean up the temporary "*-id" tag we added to claim it originally.
 *
 * @param {string} snapshotID
 * @param {string} tagValue
 * @returns {Promise}
 */
SnapToS3.prototype.markSnapshotAsCompleted = function(snapshotID, tagValue) {
	return this.ec2.deleteTags({
		Resources: [
			snapshotID
		],
		Tags: [
			{
				Key: this.options.tag + "-id"
			}
		]
	}).promise().then(() => this.ec2.createTags({
		Resources: [
			snapshotID
		],
		Tags: [
			{
				Key: this.options.tag,
				Value: tagValue
			}
		]
	}).promise());
};

/**
 * Mark the snapshot as queued up for migration.
 *
 * @param {string} snapshotID
 * @returns {Promise}
 */
SnapToS3.prototype.markSnapshotForMigration = function(snapshotID) {
	return this.ec2.createTags({
		Resources: [
			snapshotID
		],
		Tags: [
			{
				Key: this.options.tag,
				Value: "migrate"
			}
		]
	}).promise();
};

/**
 * Create a tagged temporary volume which is an image of the given snapshot, or re-use an existing volume
 * if one exists.
 *
 * @param {EC2.Snapshot} snapshot
 * @returns {Promise.<EC2.Volume>}
 */
SnapToS3.prototype.findOrCreateVolumeFromSnapshot = function(snapshot) {
	let
		logger = Logger.get(snapshot.SnapshotId);
	
	return this.findTemporaryVolumeForSnapshot(snapshot)
		.then(existingVolume => {
			if (existingVolume) {
				logger.info("A temporary volume for " + snapshot.SnapshotId + " already exists, using " + existingVolume.VolumeId);
				
				return awsTools.waitForVolumeState(this.ec2, existingVolume.VolumeId, ["available", "in-use"], 60, 10 * 1000);
			} else {
				logger.info("Creating temporary EBS volume of type \"" + this.options["volume-type"] + "\" from snapshot");
				
				return this.createTemporaryVolumeFromSnapshot(snapshot.SnapshotId)
					.then(volume => awsTools.waitForVolumeState(this.ec2, volume.VolumeId, "available", 60, 10 * 1000));
			}
		});
};

/**
 * If the volume is already attached to this instance, just return details about it, otherwise attach it and
 * return those details.
 *
 * @param {EC2.Volume} volume
 * @param {EC2.Snapshot} snapshot
 *
 * @return {Promise.<EC2.Volume>}
 */
SnapToS3.prototype.findOrAttachVolumeToInstance = function(volume, snapshot) {
	let
		logger = Logger.get(snapshot.SnapshotId),
		attachment = volume.Attachments.find(attachment => attachment.InstanceId === this.instanceIdentity.instanceId);
	
	if (attachment) {
		logger.info("Volume " + volume.VolumeId + " is already attached here");
		
		// Although the volume might have been in the Attaching state rather than Attached.
		return Promise.resolve(volume);
	}
	
	return awsTools.pickAvailableAttachmentPoint(this.ec2, this.instanceIdentity.instanceId).then(attachPoint => {
		logger.info("Attaching " + volume.VolumeId + " to this instance (" + this.instanceIdentity.instanceId + ") at " + attachPoint);
		
		return this.ec2.attachVolume({
			Device: attachPoint,
			InstanceId: this.instanceIdentity.instanceId,
			VolumeId: volume.VolumeId
		}).promise()
		// Although just because the volume is in-use, doesn't mean it will appear in the Attached state to the instance
			.then(() => awsTools.waitForVolumeState(this.ec2, volume.VolumeId, "in-use", 60, 10 * 1000));
	});
};

/**
 * Check that the given mountpoint is empty, then mount the given volume there.
 *
 * @param {BlockDevice} volume
 * @param {string} mountPoint
 * @param {ILogger} logger
 */
SnapToS3.prototype.mountTemporaryVolume = function(volume, mountPoint, logger) {
	return fsTools.forcePath(mountPoint)
		.then(() => {
			if (volume.MOUNTPOINT === mountPoint) {
				logger.info(volume.DEVICEPATH + " already mounted at " + mountPoint);
			} else {
				logger.info("Mounting " + volume.DEVICEPATH + " at " + mountPoint + "...");
				
				return fsTools.verifyDirectoryEmpty(mountPoint).then(
					() => fsTools.mountPartition(volume.DEVICEPATH, volume.FSTYPE, mountPoint, true),
					(err) => {
						throw "Mountpoint " + mountPoint + " is not empty!";
					}
				);
			}
		})
};

/**
 * Unmount a temporary volume from the given mountPoint and remove the mountPoint.
 *
 * @param {string} mountPoint
 * @param {ILogger} logger
 */
SnapToS3.prototype.unmountTemporaryVolume = function(mountPoint, logger) {
	if (!this.options["keep-temp-volumes"]) {
		logger.info("Unmounting partition...");
		
		return fsTools.unmount(mountPoint)
			.then(() => {
				try {
					fs.rmdirSync(mountPoint);
				} catch (e) {
					logger.warn("Failed to rmdir temporary mountpoint " + mountPoint + ", is it not empty? Ignoring...");
					// We don't need to stop what we're doing though
				}
			});
	} else {
		return Promise.resolve();
	}
};

/**
 *
 * @param {EC2.Snapshot} snapshot
 * @param {string} partitionName
 *
 * @returns {string}
 */
SnapToS3.prototype.decideMountpointForSnapshotPartition = function(snapshot, partitionName) {
	return this.options["mount-point"] + snapshot.SnapshotId + (partitionName.length > 0 ? "-" + partitionName : "")
};

/**
 * Upload the raw block device that is in the given list of partitions to S3 as an LZ4 compressed DD image.
 *
 * @param {BlockDevice[]} partitions
 * @param {EC2.Snapshot} snapshot
 * @param {S3.TagList} tags
 * @param {S3.PutObjectRequest} s3Params - S3 params object to pass to the S3 upload class
 * @param {ILogger} logger
 * @returns {Promise}
 */
SnapToS3.prototype.uploadPartitionsUsingDd = function(partitions, snapshot, tags, s3Params, logger) {
	s3Params.Key = createS3KeyForSnapshotVolumeImage(snapshot);
	
	let
		drive = filterBlockDevicesToGetRawDisk(partitions),
		
		dd = child_process.spawn("dd", ["bs=256k", "if=" + drive.DEVICEPATH], {
			stdio: ["ignore", "pipe", "pipe"]
		}),
		
		promise = this.uploadProcessStdOut(dd, drive.SIZE, tags, s3Params, logger);
	
	if (this.options.validate) {
		promise = promise
			.then(() =>
				this.s3.headObject({
					Bucket: s3Params.Bucket,
					Key: s3Params.Key
				}).promise()
			)
			.then(ddObjectHead => {
				logger.info("Validating the upload of this volume...");
				
				return this.validateFileAgainstCompressedS3File(drive.DEVICEPATH, drive.SIZE, s3Params.Key, ddObjectHead.ContentLength);
			})
			.then(hash => this.reportVolumeDdHashSuccess(snapshot, hash, logger));
	}
	
	return promise;
};

/**
 *
 * @param {BlockDevice} partition
 * @param {EC2.Snapshot} snapshot
 * @param {Object} hashes
 * @param {ILogger} logger
 */
SnapToS3.prototype.reportPartitionTarHashSuccess = function(partition, snapshot, hashes, logger) {
	this.emit("debug", {
		event: "tarHashesVerified",
		snapshotID: snapshot.SnapshotId,
		partitionName: partition.PARTNAME,
		hashes: hashes
	});
	
	logger.info("MD5 of all " + Object.keys(hashes.localHashes).length + " files match");
};

SnapToS3.prototype.reportVolumeDdHashSuccess = function(snapshot, hash, logger) {
	this.emit("debug", {
		event: "ddHashVerified",
		snapshotID: snapshot.SnapshotId,
		hash: hash
	});
	
	logger.info("The MD5 of the original snapshot (" + hash + ") successfully matches the copy in S3!");
};

/**
 * Tar up each of the given partitions, compress with LZ4, and upload to S3.
 *
 * @param {BlockDevice[]} partitions
 * @param {EC2.Snapshot} snapshot
 * @param {S3.TagList} tags - S3 tags to add to the resulting objects
 * @param {S3.PutObjectRequest} s3Params - S3 params object to pass to the S3 upload class
 * @param {ILogger} logger
 * @returns {Promise}
 */
SnapToS3.prototype.uploadPartitionsUsingTar = function(partitions, snapshot, tags, s3Params, logger) {
	partitions = filterBlockDevicesToGetFilesystems(partitions);
	
	logger.info(partitions.length + " partition" + (partitions.length > 1 ? "s" : "") + " to upload");
	logger.info("");
	
	let
		promise = Promise.resolve();
	
	// Tar up the files in each partition separately
	partitions.forEach((partition, partitionIndex) => {
		promise = promise.then(() => {
			let
				mountPoint = this.decideMountpointForSnapshotPartition(snapshot, partition.PARTNAME),
				mountSize,
				promise;
			
			s3Params.Key = createS3KeyForSnapshotPartitionTar(snapshot, partition.PARTNAME);
			
			logger.info("Uploading partition " + (partitionIndex + 1) + " of " + partitions.length + "...");
			
			promise = this.mountTemporaryVolume(partition, mountPoint, logger)
				.then(() => {
					this.emit("debug", {
						event: "temporaryPartitionMounted",
						snapshotID: snapshot.SnapshotId,
						devicePath: partition.DEVICEPATH,
						mountPoint: mountPoint,
						name: partition.PARTNAME
					});
				
					logger.info("Computing size of files to upload...");
					return fsTools.getRecursiveFileSize(mountPoint);
				})
				.then((_mountSize) => {
					const
						tar = child_process.spawn("tar", ["-c", "."], {
							cwd: mountPoint,
							stdio: ["ignore", "pipe", "pipe"]
						});
					
					mountSize = _mountSize;
					
					return this.uploadProcessStdOut(tar, mountSize, tags, s3Params, logger)
				});
			
			if (this.options.validate) {
				promise = promise
					.then(() => {
						logger.info("Upload complete, now validating the upload of this partition...");
						
						return this.s3.headObject({
							Bucket: s3Params.Bucket,
							Key: s3Params.Key
						}).promise().then(
							s3Head => this.validateDirectoryAgainstS3Tar(mountPoint, mountSize, s3Params.Key, s3Head.ContentLength),
							error => {
								throw "\"s3://" + s3Params.Bucket + "/" + s3Params.Key + "\" should exist, but wasn't readable/found! " + error;
							}
						).then(hashes => this.reportPartitionTarHashSuccess(partition, snapshot, hashes, logger));
					});
			}
			
			promise = promise
				.then(() => {
					logger.info("Uploaded partition to S3 successfully!");
					
					return this.unmountTemporaryVolume(mountPoint, logger);
				})
				.then(() => logger.info(""));
			
			return promise;
		});
	});
	
	return promise;
};

/**
 * Upload the given volume, which is currently attached to this instance and is based on the given snapshot, to S3.
 *
 * @param {EC2.Volume} volume
 * @param {EC2.Snapshot} snapshot
 *
 * @returns {Promise}
 */
SnapToS3.prototype.uploadTemporaryVolume = function(volume, snapshot) {
	const
		logger = Logger.get(snapshot.SnapshotId),
		
		/**
		 * @type {S3.PutObjectRequest}
		 */
		s3Params = {
			Bucket: this.options.bucket,
			Metadata: {
				"snapshot-starttime": moment(snapshot.StartTime).format(), // Defaults to ISO8601
				"snapshot-snapshotid": snapshot.SnapshotId,
				"snapshot-volumesize": "" + snapshot.VolumeSize,
				"snapshot-volumeid": snapshot.VolumeId,
				"snapshot-description": snapshot.Description,
			}
		},
	
		tags = this.sanitiseSnapshotTagsForS3(snapshot.Tags);
	
	if (this.options.sse) {
		s3Params.ServerSideEncryption = this.options.sse;
		
		if (this.options["sse-kms-key-id"] !== undefined) {
			s3Params.SSEKMSKeyId = this.options["sse-kms-key-id"];
		}
	}
	
	logger.info("Waiting for " + volume.VolumeId + "'s partitions to become visible to the operating system...");
	
	return awsTools.waitForVolumePartitions(volume, this.instanceIdentity.instanceId, PARTITION_POLL_MAX_RETRY, PARTITION_POLL_INTERVAL)
		.then(partitions => {
			if (this.options.dd) {
				return this.uploadPartitionsUsingDd(partitions, snapshot, tags, s3Params, logger);
			} else {
				return this.uploadPartitionsUsingTar(partitions, snapshot, tags, s3Params, logger);
			}
		});
};

/**
 * Checks that the MD5 hash of the given S3 object is equal to the hash of the local file after the S3 object is
 * decompressed with LZ4.
 *
 * @param {string} filename - Local file/device to be hashed
 * @param {int} fileSize - Size of file in bytes
 * @param {string} s3Key - S3 object to be decompressed and hashed
 * @param {int} s3Size - Size of object in S3
 *
 * @returns Promise.<string> - The MD5 hash that both local and remote files hash to
 */
SnapToS3.prototype.validateFileAgainstCompressedS3File = function(filename, fileSize, s3Key, s3Size) {
	return new Promise((resolve, reject) => {
		let
			localBytesReadSinceLastStatus = 0,
			localBytesReadTotal = 0,
			
			s3BytesReadSinceLastStatus = 0,
			s3BytesReadTotal = 0;
		
		const
			fileStream = fs.createReadStream(filename, {
				flags: "r",
				encoding: null,
			}),
			s3Stream = this.s3.getObject({
				Bucket: this.options.bucket,
				Key: s3Key
			}).createReadStream(),
			
			localProgress = new SimpleProgressStream(),
			s3Progress = new SimpleProgressStream(),
			
			// Show the summed progress of both the local and remote hash operations on a single bar
			barMaximum = Math.floor((fileSize + s3Size) / 1024),
			
			bar = new ProgressBar('    hashing [:bar] :rate KB/s :eta, S3: :s3Progress% Local: :localProgress%', {
				complete: '=',
				incomplete: ' ',
				width: 32,
				total: barMaximum,
				renderThrottle: PROGRESS_BAR_UPDATE_RATE
			}),
			
			updateBar = () => {
				if (!bar.complete) {
					bar.tick((localBytesReadSinceLastStatus + s3BytesReadSinceLastStatus) / 1024, {
						s3Progress: Math.floor(s3BytesReadTotal / s3Size * 100),
						localProgress: Math.floor(localBytesReadTotal / fileSize * 100)
					});
				}
				
				localBytesReadSinceLastStatus = 0;
				s3BytesReadSinceLastStatus = 0;
			};
		
		const
			// Pipeline for S3 hashing:
			s3Promise = new Promise((resolve, reject) => {
				 const
					 lz4 = child_process.spawn("lz4", ["-d"], {
						 stdio: ["pipe", "pipe", process.stderr]
					 }),
					 
					 hasher = crypto.createHash("md5");
					
				 hasher.on("readable", function() {
					const
						data = hasher.read();
					
					if (data) {
						let
							hex = data.toString("hex");
						
						if (hex.length === 32) {
							resolve(hex);
						} else {
							reject("Bad MD5 hash calculated for S3");
						}
					}
				});
				
				for (let stream of [s3Stream, lz4.stdin, lz4.stdout, s3Progress, hasher]) {
					stream.on("error", reject);
				}
				
				s3Progress.on("progress", (bytesRead) => {
					s3BytesReadSinceLastStatus += bytesRead;
					s3BytesReadTotal += bytesRead;
					
					updateBar();
				});
				
				s3Stream.pipe(s3Progress).pipe(lz4.stdin);
				lz4.stdout.pipe(hasher);
			}),
			
			// Pipeline for local file hashing:
			localPromise = new Promise((resolve, reject) => {
				const
					hasher = crypto.createHash("md5");
				
				hasher.on("readable", function () {
					const
						data = hasher.read();
					
					if (data) {
						let
							hex = data.toString("hex");
						
						if (hex.length === 32) {
							resolve(hex);
						} else {
							reject("Bad MD5 hash calculated for local file");
						}
					}
				});
				
				for (let stream of [fileStream, localProgress, hasher]) {
					stream.on("error", reject);
				}
				
				localProgress.on("progress", (bytesRead) => {
					localBytesReadSinceLastStatus += bytesRead;
					localBytesReadTotal += bytesRead;
					
					updateBar();
				});
				
				fileStream.pipe(localProgress).pipe(hasher);
			});
		
		bar.render({
			s3Progress: 0,
			localProgress: 0
		});
		
		// Wait for the local file and S3 to both be hashed:
		Promise.all([s3Promise, localPromise]).then(
			hashes => {
				if (!bar.complete) {
					bar.update(1.0, {
						s3Progress: 100,
						localProgress: 100
					});
				}
				
				if (hashes[0] === hashes[1]) {
					resolve(hashes[0]);
				} else {
					reject("Hash of decompressed S3 object (" + hashes[0] + ") does not match local temporary volume (" + hashes[1] + ")");
				}
			},
			error => {
				updateBar();
				if (!bar.complete) {
					bar.terminate();
				}
				
				reject(error);
			}
		);
	});
};

/**
 * @typedef {Object} TarS3ComparisonHashes
 * @property {Object} s3Hashes
 * @property {Object} localHashes
 */

/**
 * Check that the hash of the files in the given directory matches those of the LZ4 compressed tar at the given
 * S3 location.
 *
 * @param {string} directory
 * @param {int} directorySize - Total size in bytes of files in directory (for progress bar)
 * @param {string} s3Key
 * @param {int} s3Size - Download size of S3 object (for progress bar)
 * @returns {Promise.<TarS3ComparisonHashes>}
 */
SnapToS3.prototype.validateDirectoryAgainstS3Tar = function(directory, directorySize, s3Key, s3Size) {
	return new Promise((resolve, reject) => {
		let
			localBytesReadSinceLastStatus = 0,
			localBytesReadTotal = 0,
			
			s3BytesReadSinceLastStatus = 0,
			s3BytesReadTotal = 0;
		
		const
			s3Stream = this.s3.getObject({
				Bucket: this.options.bucket,
				Key: s3Key
			}).createReadStream(),
			
			s3Progress = new SimpleProgressStream(),
			
			// Show the summed progress of both the local and remote hash operations on a single bar
			barMaximum = Math.floor((directorySize + s3Size) / 1024),
			
			bar = new ProgressBar('    hashing [:bar] :rate KB/s :eta, S3: :s3Progress% Local: :localProgress%', {
				complete: '=',
				incomplete: ' ',
				width: 32,
				total: barMaximum,
				renderThrottle: PROGRESS_BAR_UPDATE_RATE
			}),
			
			updateBar = () => {
				if (!bar.complete) {
					bar.tick((localBytesReadSinceLastStatus + s3BytesReadSinceLastStatus) / 1024, {
						s3Progress: Math.floor(s3BytesReadTotal / s3Size * 100),
						localProgress: Math.floor(localBytesReadTotal / directorySize * 100)
					});
				}
				
				localBytesReadSinceLastStatus = 0;
				s3BytesReadSinceLastStatus = 0;
			};
		
		const
			// S3 tar hashing:
			s3Promise = (() => {
				const
					lz4 = child_process.spawn("lz4", ["-d"], {
						stdio: ["pipe", "pipe", process.stderr]
					});
				
				for (let stream of [s3Stream, lz4.stdin, lz4.stdout, s3Progress]) {
					stream.on("error", reject);
				}
				
				s3Progress.on("progress", (bytesRead) => {
					s3BytesReadSinceLastStatus += bytesRead;
					s3BytesReadTotal += bytesRead;
					
					updateBar();
				});
				
				s3Stream.pipe(s3Progress).pipe(lz4.stdin);
				
				return fsTools.hashTarFilesFromStream(lz4.stdout);
			})(),
			
			// Local file hashing:
			localPromise = fsTools.hashFilesInDirectory(directory, (bytesRead) => {
				localBytesReadSinceLastStatus += bytesRead;
				localBytesReadTotal += bytesRead;
				updateBar();
			});
		
		bar.render({
			s3Progress: 0,
			localProgress: 0
		});
		
		// Wait for the local files and S3 to both be hashed:
		Promise.all([s3Promise, localPromise]).then(
			hashes => {
				if (!bar.complete) {
					bar.update(1.0, {
						s3Progress: 100,
						localProgress: 100
					});
				}
				
				let
					errors = fsTools.compareFileTreeHashes(hashes[0], "S3 tar", hashes[1], "temporary volume");
				
				if (errors.length === 0) {
					resolve({s3Hashes: hashes[0], localHashes: hashes[1]});
				} else {
					reject("Local directory \"" + directory + "\" and contents of S3 tar \"s3://" + this.options.bucket + "/" + s3Key + "\" differ!\n" + errors.join("\n"));
				}
			},
			error => {
				updateBar();
				if (!bar.complete) {
					bar.terminate();
				}
				
				reject(error);
			}
		);
	});
};

SnapToS3.prototype.validatePartitionsUsingDd = function(partitions, snapshot, s3Key, s3Size, logger) {
	let
		drive = filterBlockDevicesToGetRawDisk(partitions);
	
	logger.info("Comparing the MD5 of the original " + snapshot.SnapshotId + " (" + drive.DEVICEPATH + ") with the S3 copy \"s3://" + this.options.bucket + "/" + s3Key + "\"...");
	
	return this.validateFileAgainstCompressedS3File(drive.DEVICEPATH, drive.SIZE, s3Key, s3Size)
		.then(hash => this.reportVolumeDdHashSuccess(snapshot, hash, logger));
};

SnapToS3.prototype.validatePartitionsUsingTar = function(partitions, snapshot, logger) {
	partitions = filterBlockDevicesToGetFilesystems(partitions);
	
	logger.info(partitions.length + " partition" + (partitions.length > 1 ? "s" : "") + " to validate");
	logger.info("");
	
	let
		promise = Promise.resolve();
	
	partitions.forEach((partition, partitionIndex) => {
		promise = promise
			.then(() => logger.info("Validating partition " + (partitionIndex + 1) + " of " + partitions.length + "..."))
			.then(() => {
				let
					mountPoint = this.decideMountpointForSnapshotPartition(snapshot, partition.PARTNAME),
					s3Key = createS3KeyForSnapshotPartitionTar(snapshot, partition.PARTNAME);
				
				return this.s3.headObject({
					Bucket: this.options.bucket,
					Key: s3Key
				}).promise().then(
					s3Head => this.mountTemporaryVolume(partition, mountPoint, logger)
						.then(() => fsTools.getRecursiveFileSize(mountPoint))
						.then(mountSize => this.validateDirectoryAgainstS3Tar(mountPoint, mountSize, s3Key, s3Head.ContentLength))
						.then(hashes => this.reportPartitionTarHashSuccess(partition, snapshot, hashes, logger))
						.then(() => this.unmountTemporaryVolume(mountPoint, logger)),
					error => {
						throw "\"s3://" + this.options.bucket + "/" + s3Key + "\" should exist, but wasn't readable/found! " + error;
					}
				);
			})
			.then(() => logger.info(""));
	});
	
	return promise;
};

/**
 * Validate the given volume, which is based on the given snapshot, against uploaded files from S3.
 *
 * @param {EC2.Volume} volume
 * @param {EC2.Snapshot} snapshot
 *
 * @returns {Promise}
 */
SnapToS3.prototype.validateTemporaryVolume = function(volume, snapshot) {
	const
		logger = Logger.get(snapshot.SnapshotId),
		ddKey = createS3KeyForSnapshotVolumeImage(snapshot);
	
	// Check if we have a dd image in S3 to validate against
	return this.s3.headObject({
		Bucket: this.options.bucket,
		Key: ddKey
	}).promise()
		.catch(err => null) // If we failed to find a DD image then assume it was a set of part tars
		.then(ddObjectHead => {
			logger.info("Waiting for " + volume.VolumeId + "'s partitions to become visible to the operating system...");
			
			return awsTools.waitForVolumePartitions(volume, this.instanceIdentity.instanceId, PARTITION_POLL_MAX_RETRY, PARTITION_POLL_INTERVAL)
				.then(partitions => {
					if (ddObjectHead) {
						return this.validatePartitionsUsingDd(partitions, snapshot, ddKey, ddObjectHead.ContentLength, logger);
					} else {
						return this.validatePartitionsUsingTar(partitions, snapshot, logger);
					}
				});
		});
};

SnapToS3.prototype.cleanUpTempVolume = function(volume, snapshot) {
	let
		logger = Logger.get(snapshot.SnapshotId);
	
	if (this.options["keep-temp-volumes"]) {
		logger.info("Keeping temporary volume " + volume.VolumeId + " attached");
		
		return Promise.resolve();
	} else {
		logger.info("Detaching " + volume.VolumeId);
		
		return this.ec2.detachVolume({
			VolumeId: volume.VolumeId
		}).promise()
			.then(() => awsTools.waitForVolumeState(this.ec2, volume.VolumeId, "available", 60, 10 * 1000))
			.then(() => {
				logger.info("Deleting temporary volume " + volume.VolumeId);
				
				return this.ec2.deleteVolume({
					VolumeId: volume.VolumeId
				}).promise();
			});
	}
};

/**
 *
 * @param {EC2.Snapshot} snapshot
 * @returns {Promise}
 */
SnapToS3.prototype._migrateSnapshot = function(snapshot) {
	return this.findOrCreateVolumeFromSnapshot(snapshot)
		.then(createdVolume => this.findOrAttachVolumeToInstance(createdVolume, snapshot))
		.then(attachedVolume =>
			this.uploadTemporaryVolume(attachedVolume, snapshot)
				.then(() => this.cleanUpTempVolume(attachedVolume, snapshot))
		);
};

/**
 *
 * @param {EC2.Snapshot} snapshot
 * @returns {Promise}
 */
SnapToS3.prototype._validateSnapshot = function(snapshot) {
	return this.findOrCreateVolumeFromSnapshot(snapshot)
		.then(createdVolume => this.findOrAttachVolumeToInstance(createdVolume, snapshot))
		.then(attachedVolume =>
			this.validateTemporaryVolume(attachedVolume, snapshot)
				.then(() => this.cleanUpTempVolume(attachedVolume, snapshot))
		);
};

/**
 *
 * @param {EC2.Snapshot} snapshot
 *
 * @returns {Promise}
 */
SnapToS3.prototype._claimAndMigrateSnapshot = function(snapshot) {
	let
		logger = Logger.get(snapshot.SnapshotId);
	
	logger.info("Migrating " + snapshot.SnapshotId + " to S3");
	logger.info("Tagging snapshot with \"migrating\"...");
	
	return this.raceToMarkSnapshot(snapshot.SnapshotId, "migrating")
		.then(() =>
			this._migrateSnapshot(snapshot)
				.then(
					// If we succeeded, mark the snapshot so we don't try to migrate it again
					() => {
						let
							tagWith = this.options.validate ? "validated" : "migrated";
						
						logger.info("Tagging snapshot with \"" + tagWith + "\"");
						return this.markSnapshotAsCompleted(snapshot.SnapshotId, tagWith);
					},
					// If we messed up, mark the snapshot for retry and rethrow the error up the stack
					(err) => {
						logger.info("An error occurred, tagging snapshot with \"migrate\" so it can be retried later");
						return this.markSnapshotForMigration(snapshot.SnapshotId).then(() => {
							throw err;
						});
					}
				)
		)
		.then(
			() => {
				logger.info("Successfully migrated this snapshot!");
				Logger.info(""); // No prefix on this empty line
			},
			(err) => {
				// Tag errors with the snapshot ID
				throw new SnapToS3.SnapshotMigrationError(err, snapshot.SnapshotId);
			}
		);
};

/**
 *
 * @param {EC2.Snapshot} snapshot
 *
 * @returns {Promise}
 */
SnapToS3.prototype._claimAndValidateSnapshot = function(snapshot) {
	let
		logger = Logger.get(snapshot.SnapshotId),
		originalTag;
	
	logger.info("Validating S3 against the original " + snapshot.SnapshotId);
	logger.info("Tagging snapshot with \"validating\"...");
	
	originalTag = snapshot.Tags.find(tag => tag.Key === this.options.tag);
	
	return this.raceToMarkSnapshot(snapshot.SnapshotId, "validating")
		.then(
			() => this._validateSnapshot(snapshot).then(
				// If we succeeded, mark the snapshot so we don't try to validate it again
				() => {
					logger.info("Tagging snapshot with \"validated\"");
					return this.markSnapshotAsCompleted(snapshot.SnapshotId, "validated");
				},
				// If we messed up validation, mark the snapshot for retry and rethrow the error up the stack
				(err) => {
					logger.error("Error: " + err);
					
					let
						restoreTag;
					
					if (originalTag) {
						if (originalTag.Value === "validated" || originalTag.Value === "validating") {
							/*
							 * The tag used to be in the validation stage, but validation failed, so it would be silly
							 * to put "validated" back onto the snapshot
							 */
							logger.info("An error occurred, marking the snapshot as \"migrated\"");
							restoreTag = this.markSnapshotAsCompleted(snapshot.SnapshotId, "migrated");
						} else {
							logger.info("An error occurred, restoring the original \"" + originalTag.Value + "\" tag on the snapshot");
							restoreTag = this.markSnapshotAsCompleted(snapshot.SnapshotId, originalTag.Value);
						}
					} else {
						// We might have manually told snap-to-s3 to validate a snapshot that didn't have any tag previously
						logger.info("An error occurred, clearing the \"validating\" tag from the snapshot");
						restoreTag = this.markSnapshotAsCompleted(snapshot.SnapshotId, "");
					}
					
					return restoreTag.then(() => {
						throw err;
					});
				}
			)
		)
		.then(() => {
			logger.info("Successfully validated this snapshot!");
			Logger.info(""); // No prefix on this empty line
		});
};

/**
 * Migrate the snapshots with the given snapshot descriptions.
 *
 * The migration is terminated at the first snapshot that fails to migrate.
 *
 * @param {EC2.Snapshot[]} snapshots
 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
 */
SnapToS3.prototype.migrateSnapshotsByDescription = function(snapshots) {
	let
		promise = this.initPromise;
	
	for (let snapshot of snapshots) {
		promise = promise.then(() => this._claimAndMigrateSnapshot(snapshot));
	}
	
	return promise.then(() => snapshots.map(snapshot => snapshot.SnapshotId));
};

/**
 * Find all snapshots that are eligible for migration, copy them over to S3 and mark them as migrated.
 *
 * The migration is terminated at the first snapshot that fails to migrate.
 *
 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
 */
SnapToS3.prototype.migrateAllTaggedSnapshots = function() {
	return this.initPromise.then(() => {
		let
			migratedSnapshotIDs = [];
		
		const
			migrateOne = () => this.findMigratableSnapshots().then(snapshots => {
				if (snapshots.length === 0) {
					return migratedSnapshotIDs;
				}
				
				/*
				 * Only migrate one snapshot now, as the migratable state of the remaining snapshots might change while
				 * we're migrating this one!
				 */
				let
					snapshot = snapshots[0]; // Could pick randomly here to reduce collisions with other processes
				
				return this.migrateSnapshotsByDescription([snapshot])
					.then(() => {
						migratedSnapshotIDs.push(snapshot.SnapshotId);
						
						return migrateOne(); // Go again to migrate any remaining snapshots
					});
			});
		
		return migrateOne();
	});
};

/**
 * Find one snapshot that is eligible for migration, copy it over to S3 and mark it as migrated.
 *
 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
 */
SnapToS3.prototype.migrateOneTaggedSnapshot = function() {
	return this.initPromise
		.then(() => this.findMigratableSnapshots())
		.then(snapshots => {
			if (snapshots.length === 0) {
				return [];
			}
			
			return this.migrateSnapshotsByDescription([snapshots[0]]);
		});
};

/**
 * Migrate the specified snapshots to S3
 *
 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
 */
SnapToS3.prototype.migrateSnapshots = function(snapshotIDs) {
	return this.initPromise
		.then(() => describeSnapshots(this.ec2, snapshotIDs))
		.then(snapshots => this.migrateSnapshotsByDescription(snapshots));
};

/**
 * @param {EC2.Snapshot[]} snapshots
 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
 */
SnapToS3.prototype.validateSnapshotsByDescription = function(snapshots) {
	let
		promise = this.initPromise,
		successes = [], failures = {};
	
	for (let snapshot of snapshots) {
		promise = promise
			.then(() => this._claimAndValidateSnapshot(snapshot))
			.then(
				() => {
					successes.push(snapshot.SnapshotId);
				},
				error => {
					failures[snapshot.SnapshotId] = error;
					
					// And keep on validating the rest, we'll report the errors later.
				}
			);
	}
	
	return promise.then(() => {
		if (Object.keys(failures).length > 0) {
			throw new SnapshotValidationError(failures, successes);
		}
		
		return successes;
	});
};

/**
 * Find all snapshots that are eligible for validation, validate them against S3, and mark them as validated if successful.
 *
 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
 */
SnapToS3.prototype.validateAllTaggedSnapshots = function() {
	return this.initPromise.then(() => {
		let
			validatedSnapshotIDs = [];
		
		const
			validateOne = () => this.findValidatableSnapshots().then(snapshots => {
				if (snapshots.length === 0) {
					return validatedSnapshotIDs;
				}
				
				/*
				 * Only validate one snapshot now, as the validatable state of the remaining snapshots might change while
				 * we're migrating this one!
				 */
				let
					snapshot = snapshots[0];
				
				return this.validateSnapshotsByDescription([snapshot])
					.then(success => {
						validatedSnapshotIDs = validatedSnapshotIDs.concat(success);
						return validateOne(); // Go again to migrate any remaining snapshots
					});
			});
		
		return validateOne();
	});
};

/**
 * Find one snapshot that is eligible for validation, validate it against S3, and mark as validated.
 *
 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
 */
SnapToS3.prototype.validateOneTaggedSnapshot = function() {
	return this.initPromise
		.then(() => this.findValidatableSnapshots())
		.then(snapshots => {
			if (snapshots.length === 0) {
				return [];
			}
			
			/*
			 * We could pick at random, rather than always picking the first, to reduce chance of collisions between
			 * processes:
			 */
			return this.validateSnapshotsByDescription([snapshots[0]]);
		});
};

/**
 * Validate the specified snapshots against S3, and mark them as validated if successful.
 *
 * @param {String[]} snapshotIDs
 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
 */
SnapToS3.prototype.validateSnapshots = function(snapshotIDs) {
	if (snapshotIDs.length === 0) {
		throw "No snapshot IDs were provided to validate!";
	}
	
	return this.initPromise
		.then(() => describeSnapshots(this.ec2, snapshotIDs))
		.then(snapshots => this.validateSnapshotsByDescription(snapshots));
};


class SnapshotsMissingError extends Error {
	constructor(snapshotIDs) {
		super("These snapshots could not be found/read: " + snapshotIDs.join(", "));
		
		this.snapshotIDs = snapshotIDs;
	}
}

class SnapshotMigrationError extends Error {
	constructor(error, snapshotID) {
		super(snapshotID + ": " + error);
		
		this.error = error;
		this.snapshotID = snapshotID;
	}
}

class SnapshotValidationError extends Error {
	/**
	 *
	 * @param {Object} failures - A map from snapshot-id to Error
	 * @param {string[]} successes - An array of snapshot ids
	 */
	constructor(failures, successes) {
		super("These snapshots failed to validate: " + Object.keys(failures).map(snapshotID => snapshotID + ": " + failures[snapshotID]).join(", "));
		
		this.failures = failures;
		this.successes = successes;
	}
}

SnapToS3.SnapshotMigrationError = SnapshotMigrationError;
SnapToS3.SnapshotValidationError = SnapshotValidationError;
SnapToS3.SnapshotsMissingError = SnapshotsMissingError;

module.exports = SnapToS3;