"use strict";

const
	crypto = require("crypto"),
	fs = require("fs"),
	child_process = require("child_process"),
	querystring = require("querystring"),
	assert = require("assert"),
	which = require("which"),
	
	clone = require("clone"),
	Logger = require("js-logger"),
	filesize = require("filesize"),
	moment = require("moment"),
	ProgressBar = require("progress"),
	ProgressStream = require("progress-stream"),
	
	AWS = require("aws-sdk"),
	fsTools = require("./filesystem-tools"),
	
	metadataService = new AWS.MetadataService();

const
	TAG_RACE_DELAY = 4000;

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
 * Wait for the given volume to enter the specified state. Resolves to a description of the volume.
 *
 * @param {Object} ec2
 * @param {string} volumeID
 * @param {string|string[]} states - One or more acceptable states to wait for
 * @param {int} maxRetries
 * @param {int} retryInterval - Milliseconds
 * @returns {Promise.<EC2.Volume>}
 */
function waitForVolumeState(ec2, volumeID, states, maxRetries, retryInterval) {
	if (!Array.isArray(states)) {
		states = [states];
	}
	
	return new Promise(function(resolve, reject) {
		const
			attempt = function() {
				ec2.describeVolumes({
					VolumeIds: [
						volumeID
					]
				}).promise().then(
					data => {
						for (let volume of data.Volumes) {
							if (states.indexOf(volume.State) !== -1) {
								resolve(volume);
								return;
							}
						}
						
						if (maxRetries <= 0) {
							reject("Timed out waiting for " + volumeID + " to enter state '" + state + "', currently in state '" + data.Volumes[0].State + "'.");
						} else {
							maxRetries--;
							setTimeout(attempt, retryInterval || 4000);
						}
					},
					error => reject
				);
			};
		
		attempt();
	});
}

/**
 * Wait for the given volume to appear as a block device in our OS.
 *
 * @param {EC2.Volume} volume
 * @param {string} instanceID
 * @param {int} maxRetries
 * @param {int} retryInterval
 *
 * @returns {Promise.<BlockDevice[]>} Resolves to an array of details of the discovered partitions.
 */
function waitForVolumePartitions(volume, instanceID, maxRetries, retryInterval) {
	return new Promise(function(resolve, reject) {
		let
			attempt = function() {
				identifyPartitionsForAttachedVolume(volume, instanceID).then(
					partitions => {
						if (partitions.length === 0) {
							if (maxRetries <= 0) {
								reject("Timed out waiting for " + volume.VolumeId + " to appear in /dev");
							} else {
								maxRetries--;
								setTimeout(attempt, retryInterval || 4000);
							}
						} else {
							resolve(partitions);
						}
					},
					error => reject
				);
			};
		
		attempt();
	});
}

/**
 * Find an attachment point on this instance which is not already occupied with another block device.
 *
 * @param ec2
 * @param {String} instanceID
 * @returns {Promise.<String>} The path of the found attachment point
 */
function pickAvailableAttachmentPoint(ec2, instanceID) {
	return ec2.describeInstanceAttribute({
		Attribute: "blockDeviceMapping",
		InstanceId: instanceID
	}).promise().then(function(data) {
		let
			availableLetters = {};
		
		for (let i = "f".charCodeAt(0); i <= "z".charCodeAt(0); i++) {
			availableLetters[String.fromCharCode(i)] = true;
		}
		
		for (let mapping of data.BlockDeviceMappings) {
			let
				matches = mapping.DeviceName.match(/^\/dev\/(?:sd|xvd)([a-zA-Z])/);
			
			if (matches) {
				availableLetters[matches[1].toLowerCase()] = false;
			}
		}
		
		availableLetters = Object.keys(availableLetters).filter(letter => availableLetters[letter]);
		
		if (availableLetters.length === 0) {
			throw "No drive attachment points are available on this instance!";
		}
		
		// Pick an available drive letter at random so that concurrent executions are unlikely to collide
		let
			random;
		
		do {
			random = crypto.randomBytes(1)[0];
		} while (random >= availableLetters.length);
		
		return "/dev/sd" + availableLetters[random];
	});
}

/**
 * Get the volume's attachment information to the given instance, or null if the volume is not described as being
 * attached to that instance.
 *
 * @param {EC2.Volume} volume
 * @param {String} instanceID
 *
 * @returns {EC2.VolumeAttachment}
 */
function getVolumeAttachmentToInstance(volume, instanceID) {
	return volume.Attachments.find(attachment => attachment.InstanceId === instanceID);
}

function createS3KeyForPartitionTar(snapshot, partitionName) {
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

function createS3KeyForVolumeImage(snapshot) {
	let
		filename = snapshot.VolumeId + "/" + moment(snapshot.StartTime).format() + " " + snapshot.SnapshotId;
	
	if (snapshot.Description.length > 0) {
		filename += " - " + snapshot.Description;
	}

	return filename + ".img.lz4";
}

/**
 * Find a list of the partitions associated with the given volume that are visible to our operating system.
 *
 * @param {EC2.Volume} volume
 * @param {String} instanceID
 * @returns {Promise.<BlockDevice[]>}
 */
function identifyPartitionsForAttachedVolume(volume, instanceID) {
	let
		attachment = getVolumeAttachmentToInstance(volume, instanceID);
	
	if (attachment) {
		return fsTools.listBlockDevices().then(devices => {
			let
				// Just extract the drive letter / partition number from the attachment point reported by EC2:
				attachedDeviceSuffix = attachment.Device.replace("/dev/sd", "").replace("/dev/xvd", ""),
				
				// That drive could appear to be called a couple of different things depending on our OS:
				acceptablePrefixes = ["sd" + attachedDeviceSuffix, "xvd" + attachedDeviceSuffix];
			
			return devices.filter(device => {
				// Find all the partitions/disks that have the attachment point as their prefix
				for (let prefix of acceptablePrefixes) {
					if (device.NAME.indexOf(prefix) === 0) {
						return true;
					}
				}
				return false;
			});
		});
	}

	return Promise.resolve([]);
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
	return new Promise(function(resolve, reject) {
		let
			binaries = ["lsblk", "lz4", "tar", "du", "mount", "umount"];
		
		for (let binary of binaries) {
			try {
				which.sync(binary);
			} catch (e) {
				reject("Missing required utility '" + binary + "', is it on the path?");
				return;
			}
		}
		
		resolve();
	});
}

/**
 * Find and transfer snapshots to S3.
 *
 * @param {Object} options
 */
function SnapToS3(options) {
	this.setOptions(options);
	
	this.initPromise =
		fsTools.forcePath(this.options["mount-point"])
			.then(checkForRequiredBinaries)
			.then(fetchInstanceDetails)
			.then(instanceIdentity => {
				this.instanceIdentity = instanceIdentity;
				
				// Now AWS has been configured with our region, we can create these:
				this.s3 = new AWS.S3();
				this.ec2 = new AWS.EC2();
			});
}

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
		throw "Bad mount point";
	}
	
	this.options["compression-level"] = Math.min(Math.max(Math.round(this.options["compression-level"]), 1), 9);
};


/**
 * Compress the stdout of the given process using lz4 and upload it to s3.
 *
 * @param sourceProcess - Process to upload the stdout of
 * @param {int} streamLengthEstimate - Size of stream in bytes
 * @param {S3.TagList} tags
 * @param {Object} _s3Params - S3 params object to pass to the S3 upload class
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
			
			progressStream = new ProgressStream({
				length: streamLengthEstimate
			}),
			
			bar = new ProgressBar('  uploading [:bar] :rate KB/s :percent :eta', {
				complete: '=',
				incomplete: ' ',
				width: 32,
				total: streamLengthEstimate / 1024,
				renderThrottle: 5 * 1000
			}),
		
			// Don't modify the caller's s3Params, they might want to re-use it
			s3Params = clone(_s3Params);
		
		let
			failed = false,
			upload,
			
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
		logger.info("");
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
			tags: tags // The uploader can construct the Tagging header for us
		});
		
		upload.on("httpUploadProgress", function (/* Ignore the actual upload progress and use the disk read progress */) {
			let
				uncompressedBytes = progressStream.progress().transferred;
			
			// We'll need to refine our crude total size estimate towards the end of the upload:
			streamLengthEstimate = Math.max(uncompressedBytes, streamLengthEstimate);
			
			progressStream.setLength(streamLengthEstimate);
			
			bar.total = streamLengthEstimate / 1024;
			
			if (!failed) {
				// Avoid telling the bar to print out "100%" multiple times (it causes the bar to duplicate itself)
				if (!bar.complete) {
					bar.update(uncompressedBytes / streamLengthEstimate);
				}
			}
		});
		
		upload.promise().then(
			() => {
				process.removeListener("SIGINT", ctrlCHandler);
				
				if (!bar.complete) {
					bar.terminate();
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
				
				bar.terminate();
			}
		);
		
		/* Handle SIGINT so that CTRL+C cancels the upload rather than killing our process. This prevents incomplete
		 * multipart uploads from sitting around costing money on S3.
		 */
		process.on('SIGINT', ctrlCHandler);
	});
};

/**
 * Tar and upload a directory to S3.
 *
 * @param {String} directory - Path of directory to upload
 * @param {S3.TagList} tags
 * @param {Object} s3Params
 * @param {ILogger} logger
 *
 * @returns {Promise}
 */
SnapToS3.prototype.uploadDirectory = function(directory, tags, s3Params, logger) {
	logger.info("Computing size of files to upload...");
	
	return fsTools.getRecursiveFileSize(directory)
		.then((directorySize) => {
			const
				tar = child_process.spawn("tar", ["-c", "."], {
					cwd: directory,
					stdio: ["ignore", "pipe", "pipe"]
				});
			
			return this.uploadProcessStdOut(tar, directorySize, tags, s3Params, logger)
		});
};

/**
 * Upload an image of an entire volume to S3.
 *
 * @param {BlockDevice} disk - Disk to upload
 * @param {S3.TagList} tags
 * @param {Object} s3Params
 * @param {ILogger} logger
 *
 * @returns {Promise}
 */
SnapToS3.prototype.uploadVolumeImage = function(disk, tags, s3Params, logger) {
	const
		dd = child_process.spawn("dd", ["bs=128k", "if=" + disk.DEVICEPATH], {
			stdio: ["ignore", "pipe", "pipe"]
		});
			
	return this.uploadProcessStdOut(dd, disk.SIZE, tags, s3Params, logger);
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
		if (tag.Key !== this.options["tag"] && tag.Key !== this.options["tag"] + "-id") {
			result.push({
				Key: sanitiseS3TagValue(tag.Key),
				Value: sanitiseS3TagValue(tag.Value)
			});
		}
	}
	
	return result;
};

/**
 * Find snapshots that are ready for migration, and have been tagged as "migrate".
 *
 * @returns {Promise.<EC2.SnapshotList>}
 */
SnapToS3.prototype.findMigratableSnapshots = function() {
	return this.ec2.describeSnapshots({
		Filters: [
			{
				Name: "status",
				Values: [
					"completed"
				]
			},
			{
				Name: "tag:" + this.options["tag"],
				Values: [
					"migrate"
				]
			}
		],
		OwnerIds: [
			this.instanceIdentity.accountId
		]
	}).promise().then(data => data.Snapshots);
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
					this.options["tag"]
				]
			},
			{
				Name: "snapshot-id",
				Values: [
					snapshot.SnapshotId
				]
			}
		]
	}).promise().then(data => {
		for (/** @type EC2.Volume */let volume of data.Volumes) {
			// Make doubly sure so we don't try to steal the wrong volume...
			assert(volume.SnapshotId === snapshot.SnapshotId);
			assert(volume.Tags.find(tag => tag.Key === this.options["tag"]));
			
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
 * @param {String} availabilityZone
 * @param {String} volumeType
 *
 * @returns {Promise.<EC2.Volume>}
 */
SnapToS3.prototype.createTemporaryVolumeFromSnapshot = function(snapshotID, availabilityZone, volumeType) {
	return this.ec2.createVolume({
		SnapshotId: snapshotID,
		AvailabilityZone: availabilityZone,
		VolumeType: volumeType,
		TagSpecifications: [
			{
				ResourceType: "volume",
				Tags: [
					{
						Key: "Name",
						Value: "Temp for snap migration"
					},
					{
						Key: this.options["tag"],
						Value: 'in-progress'
					}
				]
			}
		]
	}).promise();
};

/**
 * Mark the snapshot as migration-in-progress to avoid concurrent migration attempts.
 *
 * @param {String} snapshotID
 * @returns {Promise}
 */
SnapToS3.prototype.claimSnapshotForMigration = function(snapshotID) {
	let
		randomID = crypto.randomBytes(4).readUInt32LE(0) + "";
	
	return this.ec2.createTags({
		Resources: [
			snapshotID
		],
		Tags: [
			{
				Key: this.options["tag"],
				Value: "migrating"
			},
			{
				Key: this.options["tag"] + "-id",
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
						inMigratingState = false,
						wonTheRace = false;
					
					for (let tag of data.Tags) {
						switch (tag.Key) {
							case this.options["tag"]:
								inMigratingState = tag.Value === "migrating";
								break;
							case this.options["tag"] + "-id":
								wonTheRace = tag.Value === randomID;
								break;
						}
					}
					
					if (inMigratingState && wonTheRace) {
						resolve(snapshotID);
					} else {
						reject("Another process is already migrating " + snapshotID);
					}
				},
				err => reject
			);
		}, TAG_RACE_DELAY);
	}));
};

/**
 * Mark the snapshot as migrated.
 *
 * @param {string} snapshotID
 * @returns {Promise}
 */
SnapToS3.prototype.markSnapshotAsMigrated = function(snapshotID) {
	return this.ec2.createTags({
		Resources: [
			snapshotID
		],
		Tags: [
			{
				Key: this.options["tag"],
				Value: "migrated"
			}
		]
	}).promise().then(() => this.ec2.deleteTags({
		Resources: [
			snapshotID
		],
		Tags: [
			{
				Key: this.options["tag"] + "-id"
			}
		]
	}));
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
				Key: this.options["tag"],
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
SnapToS3.prototype.createOrUseVolumeFromSnapshot = function(snapshot) {
	let
		logger = Logger.get(snapshot.SnapshotId);
	
	return this.findTemporaryVolumeForSnapshot(snapshot)
		.then(existingVolume => {
			if (existingVolume) {
				logger.info("A temporary volume for " + snapshot.SnapshotId + " already exists, using " + existingVolume.VolumeId);
				
				return waitForVolumeState(this.ec2, existingVolume.VolumeId, ["available", "in-use"], 60, 10 * 1000);
			} else {
				logger.info("Creating temporary EBS volume of type \"" + this.options["volume-type"] + "\" from snapshot");
				
				return this.createTemporaryVolumeFromSnapshot(snapshot.SnapshotId, this.instanceIdentity.availabilityZone, this.options["volume-type"])
					.then(volume => waitForVolumeState(this.ec2, volume.VolumeId, "available", 60, 10 * 1000));
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
SnapToS3.prototype.reuseOrAttachTempVolume = function(volume, snapshot) {
	let
		logger = Logger.get(snapshot.SnapshotId),
		attachment = getVolumeAttachmentToInstance(volume, this.instanceIdentity.instanceId);
	
	if (attachment) {
		logger.info("Volume " + volume.VolumeId + " is already attached here");
		
		// Although the volume might have been in the Attaching state rather than Attached.
		return Promise.resolve(volume);
	}
	
	return pickAvailableAttachmentPoint(this.ec2, this.instanceIdentity.instanceId).then(attachPoint => {
		logger.info("Attaching " + volume.VolumeId + " to this instance (" + this.instanceIdentity.instanceId + ") at " + attachPoint);
		
		return this.ec2.attachVolume({
			Device: attachPoint,
			InstanceId: this.instanceIdentity.instanceId,
			VolumeId: volume.VolumeId
		}).promise()
		// Although just because the volume is in-use, doesn't mean it will appear in the Attached state to the instance
			.then(() => waitForVolumeState(this.ec2, volume.VolumeId, "in-use", 60, 10 * 1000));
	});
};

/**
 * Mount and upload the given partition to S3.
 *
 * @param {BlockDevice} partition
 * @param {string} mountPoint
 * @param {S3.TagList} tags
 * @param {Object} s3Params
 * @param {ILogger} logger
 *
 * @returns {Promise}
 */
SnapToS3.prototype.uploadPartition = function(partition, mountPoint, tags, s3Params, logger) {
	return fsTools.forcePath(mountPoint)
		.then(() => {
			if (partition.MOUNTPOINT === mountPoint) {
				logger.info(partition.DEVICEPATH + " already mounted at " + mountPoint);
			} else {
				logger.info("Mounting " + partition.DEVICEPATH + " at " + mountPoint + "...");
				
				return fsTools.verifyDirectoryEmpty(mountPoint).then(
					() => fsTools.mountPartition(partition.DEVICEPATH, partition.FSTYPE, mountPoint),
					(err) => {
						throw "Mountpoint " + mountPoint + " is not empty!";
					}
				);
			}
		})
		.then(() => this.uploadDirectory(mountPoint, tags, s3Params, logger))
		.then(() => {
			logger.info("Uploaded partition to S3 successfully!");
			
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
			}
		});
};

/**
 * Migrate the given volume, which is based on the given snapshot, to S3.
 *
 * @param {EC2.Volume} volume
 * @param {EC2.Snapshot} snapshot
 *
 * @returns {Promise}
 */
SnapToS3.prototype.migrateTemporaryVolume = function(volume, snapshot) {
	const
		logger = Logger.get(snapshot.SnapshotId),
		
		s3Params = {
			Bucket: this.options["bucket"],
			Metadata: {
				"snapshot-starttime": moment(snapshot.StartTime).format(), // Defaults to ISO8601
				"snapshot-snapshotid": snapshot.SnapshotId,
				"snapshot-volumesize": "" + snapshot.VolumeSize,
				"snapshot-volumeid": snapshot.VolumeId,
				"snapshot-description": snapshot.Description,
			}
		},
	
		tags = this.sanitiseSnapshotTagsForS3(snapshot.Tags);
	
	return this.reuseOrAttachTempVolume(volume, snapshot)
		.then(volume => {
			// The new volume variable now contains the up-to-date attachment information
			logger.info("Waiting for " + volume.VolumeId + "'s partitions to become visible to the operating system...");
			
			return waitForVolumePartitions(volume, this.instanceIdentity.instanceId, 120, 2 * 1000)
				.then(partitions => {
					if (this.options["dd"]) {
						// Image the entire disk using dd
						let
							drives = partitions.filter(partition => partition.TYPE === "disk");
						
						assert(drives.length === 1);
						
						s3Params.Key = createS3KeyForVolumeImage(snapshot);
						
						return this.uploadVolumeImage(drives[0], tags, s3Params, logger);
					} else {
						/* If the disk had multiple partitions, presumably we have one "disk" and several "part"s.
						 * In this case, we only want the "part"s, not the raw disk too.
						 */
						if (partitions.length > 1) {
							/* We could filter by FSTYPE !== "" instead, but it might cause us to fail to report an error
							 * if there's a filesystem we don't know how to mount.
							 */
							partitions = partitions.filter(partition => partition.TYPE === "part");
						}
						
						if (partitions.length === 0) {
							throw "Couldn't find any partitions in this volume!";
						}
						
						// Tar up the files in each partition separately
						logger.info(partitions.length + " partition" + (partitions.length > 1 ? "s" : "") + " to upload");
						
						let
							promise = Promise.resolve();
						
						for (let partition of partitions) {
							promise = promise.then(() => {
								let
									mountPoint = this.options["mount-point"] + snapshot.SnapshotId + (partition.PARTNAME.length > 0 ? "-" + partition.PARTNAME : "");
								
								s3Params.Key = createS3KeyForPartitionTar(snapshot, partition.PARTNAME);
								
								return this.uploadPartition(partition, mountPoint, tags, s3Params, logger);
							});
						}
						
						return promise;
					}
				})
				.then(() => {
					if (this.options["keep-temp-volumes"]) {
						logger.info("Keeping temporary volume " + volume.VolumeId + " attached");
					} else {
						logger.info("Detaching " + volume.VolumeId);
						
						return this.ec2.detachVolume({
							VolumeId: volume.VolumeId
						}).promise()
							.then(() => waitForVolumeState(this.ec2, volume.VolumeId, "available", 60, 10 * 1000))
							.then(() => {
								logger.info("Deleting temporary volume " + volume.VolumeId);
								
								return this.ec2.deleteVolume({
									VolumeId: volume.VolumeId
								}).promise();
							});
					}
				});
		});
};

/**
 *
 * @param {EC2.Snapshot} snapshot
 * @returns {Promise}
 */
SnapToS3.prototype._migrateSnapshot = function(snapshot) {
	return this.createOrUseVolumeFromSnapshot(snapshot)
		.then(volume => this.migrateTemporaryVolume(volume, snapshot));
};

class SnapshotMigrationError extends Error {
	constructor(error, snapshotID) {
		super(snapshotID + ": " + error);
		
		this.error = error;
		this.snapshotID = snapshotID;
	}
}

SnapToS3.SnapshotMigrationError = SnapshotMigrationError;

/**
 *
 * @param {EC2.Snapshot} snapshot
 *
 * @returns {Promise}
 */
SnapToS3.prototype._claimAndMigrateSnapshot = function(snapshot) {
	let
		logger = Logger.get(snapshot.SnapshotId);
	
	Logger.info(""); // No prefix on this empty line
	
	logger.info("Migrating " + snapshot.SnapshotId + " to S3");
	logger.info("Tagging snapshot with \"migrating\"...");
	
	return this.claimSnapshotForMigration(snapshot.SnapshotId)
		.then(() =>
			this._migrateSnapshot(snapshot)
				.then(
					// If we succeeded, mark the snapshot so we don't try to migrate it again
					() => {
						logger.info("Tagging snapshot with \"migrated\"");
						return this.markSnapshotAsMigrated(snapshot.SnapshotId);
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
			},
			(err) => {
				throw new SnapToS3.SnapshotMigrationError(err, snapshot.SnapshotId);
			}
		);
};

/**
 * Find all snapshots that are eligible for migration, copy them over to S3 and mark them as migrated.
 *
 * @returns {Promise.<boolean>} Resolves to true if any snapshots were moved, false if there
 * are no snapshots eligible for migration.
 */
SnapToS3.prototype.migrateAllTaggedSnapshots = function() {
	return this.initPromise.then(() => {
		let
			migratedAny = false;
		
		const
			migrateOne = () => this.findMigratableSnapshots().then(snapshots => {
				if (snapshots.length === 0) {
					return migratedAny;
				}
				
				/*
				 * Only migrate one snapshot now, as the migratable state of the remaining snapshots might change while
				 * we're migrating this one!
				 */
				let
					snapshot = snapshots[0];
				
				return this._claimAndMigrateSnapshot(snapshot)
					.then(() => {
						migratedAny = true;
						return migrateOne(); // Go again to migrate any remaining snapshots
					});
			});
		
		return migrateOne();
	});
};

/**
 * Find one snapshot that is eligible for migration, copy it over to S3 and mark it as migrated.
 *
 * @returns {Promise.<boolean>} Resolves to true if any snapshots were moved, false if there
 * are no snapshots eligible for migration.
 */
SnapToS3.prototype.migrateOneTaggedSnapshot = function() {
	return this.initPromise
		.then(() => this.findMigratableSnapshots())
		.then(snapshots => {
			if (snapshots.length === 0) {
				return false;
			}
			
			let
				snapshot = snapshots[0];
			
			return this._claimAndMigrateSnapshot(snapshot).then(() => true);
		});
};

/**
 * Migrate the specified snapshots to S3
 *
 * @param {String[]} snapshotIDs
 * @returns {Promise}
 */
SnapToS3.prototype.migrateSnapshots = function(snapshotIDs) {
	return this.initPromise
		.then(
			() => this.ec2.describeSnapshots({
				SnapshotIds: snapshotIDs
			}).promise()
		).then(data => {
			let
				accountedFor = {},
				snapshotsMissing = false;
			
			for (let snapshot of data.Snapshots) {
				accountedFor[snapshot.SnapshotId] = true;
			}
			
			for (let snapshotID of snapshotIDs) {
				if (!snapshotID in accountedFor) {
					Logger.error("Snapshot " + snapshotID +" could not be found!");
					snapshotsMissing = true;
				}
			}
			
			if (snapshotsMissing) {
				throw "Some snapshots were not found, aborting!";
			}
			
			let
				promise = Promise.resolve();
			
			for (let snapshot of data.Snapshots) {
				promise = promise.then(() => this._claimAndMigrateSnapshot(snapshot));
			}
			
			return promise;
		})
};

/**
 * Migrate a single snapshot to S3
 *
 * @param {String} snapshotID
 * @returns {Promise}
 */
SnapToS3.prototype.migrateSnapshot = function(snapshotID) {
	return this.migrateSnapshots([snapshotID]);
};

module.exports = SnapToS3;