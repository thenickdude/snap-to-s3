const
	fs = require("fs"),
	assert = require("assert"),
	which = require("which"),
	crypto = require("crypto"),
	
	clone = require("clone"),
	Logger = require("js-logger"),
	moment = require("moment"),
	filesize = require("filesize"),
	ProgressBar = require("progress"),
	AWS = require("aws-sdk"),
	
	awsTools = require("./aws-tools"),
	fsTools = require("./filesystem-tools"),
	hashFiles = require("./hash-files"),
	SimpleProgressStream = require("./simple-progress-stream"),
	spawn = require("./spawn"),
	
	metadataService = new AWS.MetadataService();

const
	TAG_RACE_DELAY = 4000,
	PROGRESS_BAR_UPDATE_RATE = 5000, // ms
	
	PARTITION_POLL_INTERVAL = 4 * 1000, // ms
	PARTITION_POLL_MAX_RETRY = 75,
	
	VOLUME_ATTACH_POLL_MAX_RETRY = 60,
	VOLUME_ATTACH_POLL_INTERVAL = 10 * 1000;

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

/**
 *
 * @param {EC2.Snapshot} snapshot
 * @param {string} partitionName
 * @param {boolean} compressed
 * @param {boolean} encrypted
 * @returns {string}
 */
function createS3KeyForSnapshotPartitionTar(snapshot, partitionName, compressed, encrypted) {
	let
		filename = snapshot.VolumeId + "/" + moment(snapshot.StartTime).format() + " " + snapshot.SnapshotId;
		
	if (snapshot.Description.length > 0) {
		filename += " - " + snapshot.Description;
	}
	
	if (partitionName.length > 0) {
		filename += "." + partitionName;
	}
	
	return filename + ".tar" + (compressed ? ".lz4" : "") + (encrypted ? ".gpg" : "");
}

/**
 *
 * @param {EC2.Snapshot} snapshot
 * @param {boolean} compressed
 * @param {boolean} encrypted
 * @returns {string}
 */
function createS3KeyForSnapshotVolumeImage(snapshot, compressed, encrypted) {
	let
		filename = snapshot.VolumeId + "/" + moment(snapshot.StartTime).format() + " " + snapshot.SnapshotId;
	
	if (snapshot.Description.length > 0) {
		filename += " - " + snapshot.Description;
	}

	return filename + ".img" + (compressed ? ".lz4" : "") + (encrypted ? ".gpg" : "");
}

/**
 * Replace illegal characters in a tag value with underscores, and trim it to length:
 *
 * http://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html
 *
 * TODO we can probably allow all Unicode "letter", "whitespace" and "number" classes instead of this restrictive ASCII set.
 *
 * @param {string} value
 * @returns {string}
 */
function sanitiseS3TagValue(value) {
	return value.replace(/[^a-zA-Z0-9+=._:/\s-]/g, "_").substring(0, 256);
}

function hasBinaryOnPath(executableName) {
	try {
		which.sync(executableName);
		
		return true;
	} catch (e) {
		return false;
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
			throw new Error("This volume contains block devices of an unknown type (neither 'disk' nor 'part')");
		}
	}
	
	if (devices.length === 0) {
		throw new Error("Couldn't find any partitions in this volume!");
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

class SnapToS3 {
	
	/**
	 * Find and transfer snapshots to S3.
	 *
	 * @param {Object} options
	 */
	constructor(options) {
		this.setOptions(options);
		
		this.initPromise =
			fsTools.forcePath(this.options["mount-point"])
				.then(() => this._checkForRequiredBinaries())
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
	
	setOptions(options) {
		const
			optionDefaults = {
				"volume-type": "auto",
				"compression-level": 1,
				"upload-streams": 1,
				"keep-temp-volumes": false,
				"dd": false,
				"gpg-recipient": [],
				"gpg-session-key": false
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
		
		assert(Array.isArray(this.options["gpg-recipient"]));
	}
	
	_checkForRequiredBinaries() {
		let
			binaries = ["lsblk", "lz4", "tar", "du", "mount", "umount", "sort"];
	
		if (this._shouldGPGEncrypt() || this.options["gpg-session-key"] !== false) {
			binaries.push("gpg2");
		}
		
		for (let binary of binaries) {
			if (!hasBinaryOnPath(binary)) {
				throw new Error("Missing required utility '" + binary + "', is it on the path?");
			}
		}
	}
	
	_shouldGPGEncrypt() {
		return this.options["gpg-recipient"].length > 0;
	}
	
	/**
	 * Compress the stdout of the given process using lz4 and upload it to s3.
	 *
	 * @param {ProcessPromise} sourceProcess - Process to upload the stdout of
	 * @param {string} sourceProcessName - For annotating stderr output
	 * @param {int} streamLengthEstimate - Size of stream in bytes
	 * @param {S3.TagList} tags
	 * @param {S3.PutObjectRequest} _s3Params - S3 params object to pass to the S3 upload class
	 * @param {ILogger} logger
	 *
	 * @returns {Promise}
	 */
	_uploadProcessStdOut(sourceProcess, sourceProcessName, streamLengthEstimate, tags, _s3Params, logger) {
		let
			failed = false,
			uploader = null,
			progressBytesTotal = 0;
		
		const
			bar = new ProgressBar('  uploading [:bar] :rate KB/s :percent :eta', {
				complete: '=',
				incomplete: ' ',
				width: 32,
				total: Math.floor(streamLengthEstimate / 1024),
				renderThrottle: PROGRESS_BAR_UPDATE_RATE
			}),
			
			lz4 = spawn.spawnAsPromise("lz4", ["-z", "-" + this.options["compression-level"]], {
				stdio: ["pipe", "pipe", "pipe"],
				// We detach so that SIGINT won't cause lz4 to die (we'll be the ones to kill it ourselves!)
				detached: true
			}),
			
			progressStream = new SimpleProgressStream(),
			
			compressionPipeline = spawn.pipelineAsPromise(sourceProcess.stdout, progressStream, lz4.stdin),
			
			shouldEncrypt = this._shouldGPGEncrypt();
			
		let
			gpg, encryptionPipeline;
		
		if (shouldEncrypt) {
			let
				recipientArgs = [];
			
			for (let recipient of this.options["gpg-recipient"]) {
				recipientArgs.push("--recipient");
				recipientArgs.push(recipient);
			}
			
			gpg = spawn.spawnAsPromise("gpg2", ["--batch", "--encrypt", "--compress-level", "0"].concat(recipientArgs), {
				stdio: ["pipe", "pipe", "pipe"],
				detached: true
			});
			
			encryptionPipeline = spawn.pipelineAsPromise(lz4.stdout, gpg.stdin);
		} else {
			gpg = Promise.resolve();
			
			encryptionPipeline = Promise.resolve();
		}
		
		const
			finalProcess = shouldEncrypt ? gpg : lz4,
			
			uploadPromise = new Promise((resolve, reject) => {
				const
					ctrlCHandler = () => {
						reject("User aborted upload with CTRL+C (SIGINT)");
					},
					
					/**
					 * Don't modify the caller's s3Params, they might want to re-use it
					 *
					 * @type {S3.PutObjectRequest}
					 */
					s3Params = clone(_s3Params);
				
				// Upload the final result of our processing pipeline
				s3Params.Body = finalProcess.stdout;
				
				// So we'll know approximately what size of volume we can provision to restore this snapshot later
				s3Params.Metadata["uncompressed-size"] = streamLengthEstimate + "";
				
				// We don't want to resume an upload!
				assert(s3Params.UploadId === undefined);
				
				// This is the default, but let's make sure that AWS computes MD5 sums of each part for us
				AWS.config.update({
					computeChecksums: true
				});
				
				uploader = new AWS.S3.ManagedUpload({
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
				
				logger.info(filesize(streamLengthEstimate) + " to compress" + (shouldEncrypt ? ", encrypt," : "") + " and upload to s3://" + s3Params.Bucket + "/" + s3Params.Key);
				logger.info("Progress is based on the pre-compression data size:");
				
				bar.render(null);
				
				/* Handle SIGINT so that CTRL+C cancels the upload rather than killing our process. This prevents incomplete
				 * multipart uploads from sitting around costing money on S3.
				 */
				process.on('SIGINT', ctrlCHandler);
				
				uploader.promise().then(
					() => {
						process.removeListener("SIGINT", ctrlCHandler);
						
						// Do one last update to finish the bar
						if (!bar.complete) {
							bar.update(1.0);
						}
						
						resolve();
					},
					err => {
						process.removeListener("SIGINT", ctrlCHandler);
						
						bar.terminate();
						
						failed = true;
						reject(new Error("S3 upload failed: " + err));
						
						// Clean up as much as possible:
						for (let stream of [lz4.stdin, lz4.stdout, gpg.stdin, gpg.stdout, sourceProcess.stdout, progressStream]) {
							if (stream) {
								try {
									stream.end();
								} catch (e) {
									logger.error(e);
								}
							}
						}
					}
				);
			});
		
		for (let errorOutput of [[sourceProcessName, sourceProcess.stderr], ["lz4", lz4.stderr], ["gpg", gpg.stderr]]) {
			if (errorOutput[1]) {
				errorOutput[1].on("data", data => {
					/* If the upload is already failed, then we don't need to report further errors from our subprocesses.
					 *
					 * e.g. if the user cancels the download, then we don't need to print out lz4 squawking about its stdout
					 * pipe being closed.
					 */
					if (!failed) {
						for (let line of data.toString().split("\n")) {
							process.stderr.write(errorOutput[0] + ": " + line + "\n");
						}
					}
				});
			}
		}
		
		progressStream.on("progress", bytesRead => {
			progressBytesTotal += bytesRead;
			
			// We'll need to refine our crude total size estimate towards the end of the upload:
			streamLengthEstimate = Math.max(progressBytesTotal, streamLengthEstimate);
			
			bar.total = Math.floor(streamLengthEstimate / 1024);
			
			// Avoid telling the bar to print out "100%" multiple times (it causes the bar to duplicate itself)
			if (!failed && !bar.complete) {
				bar.update(progressBytesTotal / streamLengthEstimate);
			}
		});
		
		return Promise.all([uploadPromise, sourceProcess, lz4, gpg, compressionPipeline, encryptionPipeline]).then(
			results => results[0],
			err => {
				failed = true;
				
				if (uploader) {
					uploader.abort();
				}
				
				throw err;
			}
		);
	}
	
	/**
	 * S3 is rather more restrictive in character set compared to EBS/EC2 (for example, parentheses are not
	 * allowed) so we sanitise the key/value first.
	 *
	 * @param {AWS.TagList} tags
	 */
	_sanitiseSnapshotTagsForS3(tags) {
		let
			result = [];
		
		for (let tag of tags) {
			// Don't include the tags we added to the snapshot ourselves
			if (tag.Key !== this.options.tag && tag.Key !== this.options.tag + "-id" && !/^aws:/.test(tag.Key)) {
				result.push({
					Key: sanitiseS3TagValue(tag.Key),
					Value: sanitiseS3TagValue(tag.Value)
				});
			}
		}
		
		return result;
	}
	
	/**
	 * Find snapshots that are completed, and are tagged with the given value.
	 *
	 * @returns {Promise.<EC2.SnapshotList>}
	 */
	_findCompletedSnapshotsWithTag(tagValue) {
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
	}
	
	/**
	 * Find snapshots that are ready for migration.
	 *
	 * @returns {Promise.<EC2.SnapshotList>}
	 */
	_findMigratableSnapshots() {
		return this._findCompletedSnapshotsWithTag("migrate");
	}
	
	/**
	 * Find snapshots that are ready for validation.
	 *
	 * @returns {Promise.<EC2.SnapshotList>}
	 */
	_findValidatableSnapshots() {
		return this._findCompletedSnapshotsWithTag("migrated");
	}
	
	/**
	 * Find a volume which we've tagged as a temporary copy of the given snapshot, ready for copying to S3.
	 *
	 * @param {EC2.Snapshot} snapshot
	 * @returns {Promise.<EC2.Volume>}
	 */
	_findTemporaryVolumeForSnapshot(snapshot) {
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
	}
	
	/**
	 * Create a temporary EBS volume from the given snapshot.
	 *
	 * @param {String} snapshotID
	 * @param {String} [volumeType]
	 *
	 * @returns {Promise.<EC2.Volume>}
	 */
	_createTemporaryVolumeFromSnapshot(snapshotID, volumeType) {
		return this.ec2.createVolume({
			SnapshotId: snapshotID,
			AvailabilityZone: this.instanceIdentity.availabilityZone,
			VolumeType: volumeType || "standard",
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
	}
	
	/**
	 * Mark the snapshot with the given tag value. If other processes attempted to do the same thing *at a similar time*,
	 * this promise will only succeed if we won that race.
	 *
	 * @param {String} snapshotID
	 * @param {String} tagValue value to set
	 * @returns {Promise}
	 */
	_raceToMarkSnapshot(snapshotID, tagValue) {
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
							reject(new Error("Another process has already marked " + snapshotID + " as " + tagValue));
						} else {
							reject(new Error("Tried to mark " + snapshotID + " as " + tagValue + " but another process marked it with a different value"));
						}
					}
				).catch(reject);
			}, TAG_RACE_DELAY);
		}));
	}
	
	/**
	 * Mark the snapshot with the given tag and clean up the temporary "*-id" tag we added to claim it originally.
	 *
	 * @param {string} snapshotID
	 * @param {string} tagValue
	 * @returns {Promise}
	 */
	_markSnapshotAsCompleted(snapshotID, tagValue) {
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
	}
	
	/**
	 * Mark the snapshot as queued up for migration.
	 *
	 * @param {string} snapshotID
	 * @returns {Promise}
	 */
	_markSnapshotForMigration(snapshotID) {
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
	}
	
	/**
	 * Create a tagged temporary volume which is an image of the given snapshot, or re-use an existing volume
	 * if one exists.
	 *
	 * @param {EC2.Snapshot} snapshot
	 * @returns {Promise.<EC2.Volume>}
	 */
	_findOrCreateVolumeFromSnapshot(snapshot) {
		let
			logger = Logger.get(snapshot.SnapshotId);
		
		return this._findTemporaryVolumeForSnapshot(snapshot)
			.then(existingVolume => {
				if (existingVolume) {
					logger.info("A temporary volume for " + snapshot.SnapshotId + " already exists, using " + existingVolume.VolumeId);
					
					return awsTools.waitForVolumeState(this.ec2, existingVolume.VolumeId, ["available", "in-use"], 60, 10 * 1000);
				} else {
					let
						volumeType = this.options["volume-type"];
					
					if (volumeType === "auto") {
						// Standard volumes cannot exceed 1TiB:
						volumeType = snapshot.VolumeSize > 1024 ? "gp2" : "standard";
					}
					
					logger.info("Creating temporary EBS volume of type \"" + volumeType + "\" from snapshot");
					
					return this._createTemporaryVolumeFromSnapshot(snapshot.SnapshotId, volumeType)
						.then(volume => awsTools.waitForVolumeState(this.ec2, volume.VolumeId, "available", 60, 10 * 1000));
				}
			});
	}
	
	/**
	 * If the volume is already attached to this instance, just return details about it, otherwise attach it, wait for it
	 * to be attached, and return those details.
	 *
	 * @param {EC2.Volume} volume
	 * @param {EC2.Snapshot} snapshot
	 *
	 * @return {Promise.<EC2.Volume>}
	 */
	_findOrAttachVolumeToInstance(volume, snapshot) {
		let
			logger = Logger.get(snapshot.SnapshotId),
			attachment = volume.Attachments.find(attachment => attachment.InstanceId === this.instanceIdentity.instanceId);
		
		if (attachment) {
			if (attachment.State === "attached") {
				logger.info("Volume " + volume.VolumeId + " is already attached here");
				
				return Promise.resolve(volume);
			} else if (attachment.State === "attaching") {
				logger.info("Volume " + volume.VolumeId + " is already in the process of attaching...");
				
				return awsTools.waitForVolumeAttach(this.ec2, volume.VolumeId, this.instanceIdentity.instanceId, VOLUME_ATTACH_POLL_MAX_RETRY, VOLUME_ATTACH_POLL_INTERVAL);
			}
		}
		
		return new Promise((resolve, reject) => {
			let
				attemptsRemain = 10,
				
				attempt = () => {
					let
						retryInterval = Math.round(Math.random() * 5000 + 10000);
					
					attemptsRemain--;
					
					awsTools.pickAvailableAttachmentPoint(this.ec2, this.instanceIdentity.instanceId)
						.then(attachPoint => {
							logger.info("Attaching " + volume.VolumeId + " to this instance (" + this.instanceIdentity.instanceId + ") at " + attachPoint + "...");
							
							/* attachVolume can fail because somebody else stole the mountpoint from us while we weren't looking.
							 * So we retry this process several times.
							 */
							return this.ec2.attachVolume({
								Device: attachPoint,
								InstanceId: this.instanceIdentity.instanceId,
								VolumeId: volume.VolumeId
							}).promise();
						})
						.then(resolve)
						.catch(error => {
							logger.warn("Attaching volume failed: " + error);
							
							if (attemptsRemain <= 0) {
								throw new Error("Reached max retries trying to find a mountpoint for " + volume.VolumeId);
							} else {
								logger.warn("Retrying attachment in " + retryInterval + "msec (" + attemptsRemain + " attempts remain)...");
								setTimeout(attempt, retryInterval);
							}
						})
						.catch(reject);
				};
			
			attempt();
		}).then(
			// Now we've committed to an attachment point, we just need to wait for the attach to finish:
			() => awsTools.waitForVolumeAttach(this.ec2, volume.VolumeId, this.instanceIdentity.instanceId, VOLUME_ATTACH_POLL_MAX_RETRY, VOLUME_ATTACH_POLL_INTERVAL)
		);
	}
	
	/**
	 * Check that the given mountpoint is empty, then mount the given volume there.
	 *
	 * @param {BlockDevice} volume
	 * @param {string} mountPoint
	 * @param {ILogger} logger
	 */
	_mountTemporaryVolume(volume, mountPoint, logger) {
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
	}
	
	/**
	 * Unmount a temporary volume from the given mountPoint and remove the mountPoint.
	 *
	 * @param {string} mountPoint
	 * @param {ILogger} logger
	 */
	_unmountTemporaryVolume(mountPoint, logger) {
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
	}
	
	/**
	 *
	 * @param {EC2.Snapshot} snapshot
	 * @param {string} partitionName
	 *
	 * @returns {string}
	 */
	_decideMountpointForSnapshotPartition(snapshot, partitionName) {
		return this.options["mount-point"] + snapshot.SnapshotId + (partitionName.length > 0 ? "-" + partitionName : "")
	}
	
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
	_uploadPartitionsUsingDd(partitions, snapshot, tags, s3Params, logger) {
		const
			encrypted = this._shouldGPGEncrypt();
		
		s3Params.Key = createS3KeyForSnapshotVolumeImage(snapshot, true, encrypted);
		
		let
			drive = filterBlockDevicesToGetRawDisk(partitions),
			
			dd = spawn.spawnAsPromise("dd", ["bs=256k", "if=" + drive.DEVICEPATH], {
				stdio: ["ignore", "pipe", "pipe"],
				detached: true
			}),
			
			promise = this._uploadProcessStdOut(dd, "dd", drive.SIZE, tags, s3Params, logger);
		
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
					
					return this._validateFileAgainstCompressedS3File(drive.DEVICEPATH, drive.SIZE, s3Params.Key, ddObjectHead.ContentLength, encrypted);
				})
				.then(hash => this._reportVolumeDdHashSuccess(snapshot, hash, logger));
		}
		
		return promise;
	}
	
	/**
	 * @param {String} localHashesFile
	 * @param {String} remoteHashesFile
	 * @param {int} matchedFileCount
	 *
	 * @returns {Promise}
	 */
	_reportPartitionTarHashSuccess(localHashesFile, remoteHashesFile, matchedFileCount) {
		let
			promise = Promise.resolve();
		
		if (this.debugHandler) {
			promise = promise.then(() => this.debugHandler({
				event: "tarHashesVerified",
				localHashesFile: localHashesFile,
				remoteHashesFile: remoteHashesFile,
				matchedFileCount: matchedFileCount
			}));
		}
		
		return promise.then(() => matchedFileCount);
	}
	
	/**
	 *
	 * @param {EC2.Snapshot} snapshot
	 * @param {string} hash
	 * @param {ILogger} logger
	 *
	 * @returns {Promise}
	 */
	_reportVolumeDdHashSuccess(snapshot, hash, logger) {
		let
			promise = Promise.resolve();
		
		if (this.debugHandler) {
			promise = promise.then(() => this.debugHandler({
				event: "ddHashVerified",
				snapshotID: snapshot.SnapshotId,
				hash: hash
			}));
		}
		
		return promise.then(() => logger.info("The MD5 of the original snapshot (" + hash + ") successfully matches the copy in S3!"));
	}
	
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
	_uploadPartitionsUsingTar(partitions, snapshot, tags, s3Params, logger) {
		partitions = filterBlockDevicesToGetFilesystems(partitions);
		
		logger.info(partitions.length + " partition" + (partitions.length > 1 ? "s" : "") + " to upload");
		logger.info("");
		
		let
			encrypt = this._shouldGPGEncrypt(),
			promise = Promise.resolve();
		
		// Tar up the files in each partition separately
		partitions.forEach((partition, partitionIndex) => {
			promise = promise.then(() => {
				let
					mountPoint = this._decideMountpointForSnapshotPartition(snapshot, partition.PARTNAME),
					mountSize,
					promise;
				
				s3Params.Key = createS3KeyForSnapshotPartitionTar(snapshot, partition.PARTNAME, true, encrypt);
				
				logger.info("Uploading partition " + (partitionIndex + 1) + " of " + partitions.length + "...");
				
				promise = this._mountTemporaryVolume(partition, mountPoint, logger)
					.then(() => {
						if (this.debugHandler) {
							return this.debugHandler({
								event: "temporaryPartitionMounted",
								snapshotID: snapshot.SnapshotId,
								devicePath: partition.DEVICEPATH,
								mountPoint: mountPoint,
								name: partition.PARTNAME
							});
						}
					})
					.then(() => {
						logger.info("Computing size of files to upload...");
						return fsTools.getRecursiveFileSize(mountPoint);
					})
					.then((_mountSize) => {
						const
							tar = spawn.spawnAsPromise("tar", ["-c", "."], {
								cwd: mountPoint,
								stdio: ["ignore", "pipe", "pipe"],
								detached: true
							});
						
						mountSize = _mountSize;
						
						return this._uploadProcessStdOut(tar, "tar", mountSize, tags, s3Params, logger)
					});
				
				if (this.options.validate) {
					promise = promise
						.then(() => {
							logger.info("Upload complete, now validating the upload of this partition...");
							
							return this.s3.headObject({
								Bucket: s3Params.Bucket,
								Key: s3Params.Key
							}).promise();
						})
						.then(
							s3Head => this._validateDirectoryAgainstS3Tar(mountPoint, mountSize, s3Params.Key, s3Head.ContentLength, encrypt),
							error => {
								throw new Error("\"s3://" + s3Params.Bucket + "/" + s3Params.Key + "\" should exist, but wasn't readable/found! " + error);
							}
						)
						.then(matchedFileCount => logger.info("MD5 of all " + matchedFileCount + " files match"));
				}
				
				promise = promise
					.then(() => {
						logger.info("Uploaded partition to S3 successfully!");
						
						return this._unmountTemporaryVolume(mountPoint, logger);
					})
					.then(() => logger.info(""));
				
				return promise;
			});
		});
		
		return promise;
	}
	
	/**
	 * Upload the given volume, which is currently attached to this instance and is based on the given snapshot, to S3.
	 *
	 * @param {EC2.Volume} volume
	 * @param {EC2.Snapshot} snapshot
	 *
	 * @returns {Promise}
	 */
	_uploadTemporaryVolume(volume, snapshot) {
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
			
			tags = this._sanitiseSnapshotTagsForS3(snapshot.Tags);
		
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
					return this._uploadPartitionsUsingDd(partitions, snapshot, tags, s3Params, logger);
				} else {
					return this._uploadPartitionsUsingTar(partitions, snapshot, tags, s3Params, logger);
				}
			});
	}
	
	/**
	 * Checks that the MD5 hash of the given S3 object is equal to the hash of the local file after the S3 object is
	 * (optionally) decrypted and decompressed with LZ4.
	 *
	 * @param {string} filename - Local file/device to be hashed
	 * @param {int} fileSize - Size of file in bytes
	 * @param {string} s3Key - S3 object to be decompressed and hashed
	 * @param {int} s3Size - Size of object in S3
	 * @param {boolean} encrypted - True if S3 object is GPG-encrypted
	 *
	 * @returns Promise.<string> - The MD5 hash that both local and remote files hash to
	 */
	_validateFileAgainstCompressedS3File(filename, fileSize, s3Key, s3Size, encrypted) {
		let
			localBytesReadSinceLastStatus = 0,
			localBytesReadTotal = 0,
			
			s3BytesReadSinceLastStatus = 0,
			s3BytesReadTotal = 0;
		
		const
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
			},
			
			s3Pipeline = (() => {
				const
					s3Stream = this.s3.getObject({
						Bucket: this.options.bucket,
						Key: s3Key
					}).createReadStream(),
					
					progress = new SimpleProgressStream(),
					
					lz4 = spawn.spawnAsPromise("lz4", ["-d"], {
						stdio: ["pipe", "pipe", process.stderr]
					}),
					
					hash = hashFiles.md5StreamAsPromise(),
					hashPipeline = spawn.pipelineAsPromise(lz4.stdout, hash.stdin);
				
				let
					gpg, decryptPipeline, decompressPipeline;
				
				if (encrypted) {
					let gpgArguments = ["--decrypt", "--quiet", "--batch"];
					
					if (this.options["gpg-session-key"] !== false) {
						gpgArguments.push("--override-session-key");
						gpgArguments.push(this.options["gpg-session-key"]);
					}
					
					gpg = spawn.spawnAsPromise("gpg2", gpgArguments, {
						stdio: ["pipe", "pipe", process.stderr]
					});
					
					decryptPipeline = spawn.pipelineAsPromise(s3Stream, progress, gpg.stdin);
					decompressPipeline = spawn.pipelineAsPromise(gpg.stdout, lz4.stdin);
				} else {
					gpg = Promise.resolve();
					
					decryptPipeline = Promise.resolve();
					decompressPipeline = spawn.pipelineAsPromise(s3Stream, progress, lz4.stdin);
				}
				
				progress.on("progress", (bytesRead) => {
					s3BytesReadSinceLastStatus += bytesRead;
					s3BytesReadTotal += bytesRead;
					
					updateBar();
				});
				
				return Promise.all([hash, lz4, decryptPipeline, decompressPipeline, gpg, hashPipeline]).then(results => results[0]);
			})(),
			
			localPipeline = (() => {
				const
					fileStream = fs.createReadStream(filename, {
						flags: "r",
						encoding: null,
					}),
					
					progress = new SimpleProgressStream(),
					
					hash = hashFiles.md5StreamAsPromise(),
					
					pipeline = spawn.pipelineAsPromise(fileStream, progress, hash.stdin);
				
				progress.on("progress", (bytesRead) => {
					localBytesReadSinceLastStatus += bytesRead;
					localBytesReadTotal += bytesRead;
					
					updateBar();
				});
				
				return Promise.all([hash, pipeline]).then(results => results[0]);
			})();
		
		bar.render({
			s3Progress: 0,
			localProgress: 0
		});
		
		return Promise.all([s3Pipeline, localPipeline]).then(
			hashes => {
				if (!bar.complete) {
					bar.update(1.0, {
						s3Progress: 100,
						localProgress: 100
					});
				}
				
				if (hashes[0] === hashes[1]) {
					return hashes[0];
				} else {
					throw "Hash of decompressed S3 object (" + hashes[0] + ") does not match local temporary volume (" + hashes[1] + ")";
				}
			},
			error => {
				updateBar();
				if (!bar.complete) {
					bar.terminate();
				}
				
				throw error;
			}
		);
	}
	
	/**
	 * Check that the hash of the files in the given directory matches those of the LZ4 compressed tar at the given
	 * S3 location.
	 *
	 * @param {string} directory
	 * @param {int} directorySize - Total size in bytes of files in directory (for progress bar)
	 * @param {string} s3Key
	 * @param {int} s3Size - Download size of S3 object (for progress bar)
	 * @param {boolean} isGPGEncrypted - True if the S3 object is GPG-encrypted
	 * @returns {Promise.<int>} - The number of files whose hash matches (if successful)
	 */
	_validateDirectoryAgainstS3Tar(directory, directorySize, s3Key, s3Size, isGPGEncrypted) {
		return fsTools.createTempDirectory("snap-to-s3-").then(tempDirectory => {
			let
				localBytesReadSinceLastStatus = 0,
				localBytesReadTotal = 0,
				
				s3BytesReadSinceLastStatus = 0,
				s3BytesReadTotal = 0;
			
			const
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
				},
				
				localHashesFile = tempDirectory + "/local.md5",
				remoteHashesFile = tempDirectory + "/remote.md5",
				
				// S3 tar hashing:
				s3Promise = (() => {
					const
						s3Stream = this.s3.getObject({
							Bucket: this.options.bucket,
							Key: s3Key
						}).createReadStream(),
						
						progressStream = new SimpleProgressStream(),
						
						lz4 = spawn.spawnAsPromise("lz4", ["-d"], {
							stdio: ["pipe", "pipe", "pipe"]
						});
					
					let
						gpg,
						decryptPipeline, decompressPipeline;
					
					if (isGPGEncrypted) {
						let gpgArguments = ["--decrypt", "--quiet", "--batch"];
						
						if (this.options["gpg-session-key"] !== false) {
							gpgArguments.push("--override-session-key");
							gpgArguments.push(this.options["gpg-session-key"]);
						}
						
						gpg = spawn.spawnAsPromise("gpg2", gpgArguments, {
							stdio: ["pipe", "pipe", "pipe"]
						});
						
						decryptPipeline = spawn.pipelineAsPromise(s3Stream, progressStream, gpg.stdin);
						decompressPipeline = spawn.pipelineAsPromise(gpg.stdout, lz4.stdin);
					} else {
						gpg = Promise.resolve();
						
						decryptPipeline = Promise.resolve();
						decompressPipeline = spawn.pipelineAsPromise(s3Stream, progressStream, lz4.stdin);
					}
					
					for (let errorOutput of [["lz4", lz4.stderr], ["gpg", gpg.stderr]]) {
						if (errorOutput[1]) {
							errorOutput[1].on("data", data => {
								for (let line of data.toString().split("\n")) {
									process.stderr.write(errorOutput[0] + ": " + line + "\n");
								}
							});
						}
					}
					
					progressStream.on("progress", (bytesRead) => {
						s3BytesReadSinceLastStatus += bytesRead;
						s3BytesReadTotal += bytesRead;
						
						updateBar();
					});
					
					return Promise.all([
						hashFiles.hashTarFilesFromStream(lz4.stdout, remoteHashesFile),
						lz4,
						gpg,
						decompressPipeline,
						decryptPipeline
					]).then(
						results => results[0],
						error => {
							// Clean up as much as possible:
							for (let stream of [lz4.stdin, lz4.stdout, gpg.stdin, gpg.stdout, progressStream]) {
								if (stream) {
									try {
										stream.end();
									} catch (e) {
										console.error(e);
									}
								}
							}
							
							// Wait for both processes to signal failure before passing on the error
							return Promise.all([
								lz4.catch(e => null),
								gpg.catch(e => null)
							]).then(() => {
								throw error;
							});
						}
					);
				})(),
				
				// Local file hashing:
				localPromise = hashFiles.hashFilesInDirectory(directory, localHashesFile, (bytesRead) => {
					localBytesReadSinceLastStatus += bytesRead;
					localBytesReadTotal += bytesRead;
					updateBar();
				}),
				
				cleanupTempFiles = () => {
					try {
						fs.unlinkSync(localHashesFile);
						fs.unlinkSync(remoteHashesFile);
						fs.rmdirSync(tempDirectory);
					} catch (e) {
						// No big deal if we leave behind some temp files
						console.warn(e);
					}
				};
			
			bar.render({
				s3Progress: 0,
				localProgress: 0
			});
			
			// Wait for the local files and S3 to both be hashed:
			return Promise.all([s3Promise, localPromise])
				.then(
					hashedFileCounts => {
						if (!bar.complete) {
							bar.update(1.0, {
								s3Progress: 100,
								localProgress: 100
							});
						}
						
						return hashFiles.compareHashListFiles(
							localHashesFile, "temporary volume",
							remoteHashesFile, "S3 tar"
						).then(
							matchedFileCount => {
								if (matchedFileCount !== hashedFileCounts[0] || matchedFileCount !== hashedFileCounts[1]) {
									// This would indicate a bug in readHashFileAsStream() or compareSortedFileHashes()
									throw new Error("We found " + hashedFileCounts[1] + " files in the local directory and " + hashedFileCounts[0] +" in the S3 tar, but somehow after comparing " +
										"those two lists of file hashes, we only matched up " + matchedFileCount + " entries?");
								}
								return this._reportPartitionTarHashSuccess(localHashesFile, remoteHashesFile, matchedFileCount);
							},
							errors => {
								if (Array.isArray(errors)) {
									throw new Error("Local directory \"" + directory + "\" and contents of S3 tar \"s3://" + this.options.bucket + "/" + s3Key + "\" differ!\n" + errors.join("\n"));
								} else {
									throw errors;
								}
							}
						);
					},
					error => {
						updateBar();
						if (!bar.complete) {
							bar.terminate();
						}
						
						throw error;
					}
				)
				.then(
					matchedFileCount => {
						cleanupTempFiles();
						
						return matchedFileCount;
					},
					error => {
						cleanupTempFiles();
						
						throw error;
					}
				);
		});
	}
	
	_validatePartitionsUsingDd(partitions, snapshot, s3Key, s3Size, encrypted, logger) {
		let
			drive = filterBlockDevicesToGetRawDisk(partitions);
		
		logger.info("Comparing the MD5 of the original " + snapshot.SnapshotId + " (" + drive.DEVICEPATH + ") with the S3 copy \"s3://" + this.options.bucket + "/" + s3Key + "\"...");
		
		return this._validateFileAgainstCompressedS3File(drive.DEVICEPATH, drive.SIZE, s3Key, s3Size, encrypted)
			.then(hash => this._reportVolumeDdHashSuccess(snapshot, hash, logger));
	}
	
	_validatePartitionsUsingTar(partitions, snapshot, logger) {
		partitions = filterBlockDevicesToGetFilesystems(partitions);
		
		logger.info(partitions.length + " partition" + (partitions.length > 1 ? "s" : "") + " to validate");
		logger.info("");
		
		let
			promise = Promise.resolve(),
			isEncrypted = false;
		
		partitions.forEach((partition, partitionIndex) => {
			promise = promise
				.then(() => logger.info("Validating partition " + (partitionIndex + 1) + " of " + partitions.length + "..."))
				.then(() => {
					let
						mountPoint = this._decideMountpointForSnapshotPartition(snapshot, partition.PARTNAME),
						s3Key = createS3KeyForSnapshotPartitionTar(snapshot, partition.PARTNAME, true, isEncrypted);

					return this.s3.headObject({
						Bucket: this.options.bucket,
						Key: s3Key
					}).promise()
						.catch(err => {
							// Unencrypted version not found, look for an encrypted one instead (or vice-versa)
							isEncrypted = !isEncrypted;
							s3Key = createS3KeyForSnapshotPartitionTar(snapshot, partition.PARTNAME, true, isEncrypted);
							
							return this.s3.headObject({
								Bucket: this.options.bucket,
								Key: s3Key
							}).promise();
						})
						.then(
							s3Head => this._mountTemporaryVolume(partition, mountPoint, logger)
								.then(() => {
									logger.info("Computing size of files to validate...");
									return fsTools.getRecursiveFileSize(mountPoint);
								})
								.then(mountSize => {
									logger.info(filesize(mountSize) + " to be validated...");
									
									return this._validateDirectoryAgainstS3Tar(mountPoint, mountSize, s3Key, s3Head.ContentLength, isEncrypted);
								})
								.then(fileCount => logger.info("MD5 of all " + fileCount + " files match"))
								.then(() => this._unmountTemporaryVolume(mountPoint, logger)),
							error => {
								throw "\"s3://" + this.options.bucket + "/" + s3Key + "\" should exist, but wasn't readable/found! " + error;
							}
						);
				})
				.then(() => logger.info(""));
		});
		
		return promise;
	}
	
	/**
	 * Validate the given volume, which is based on the given snapshot, against uploaded files from S3.
	 *
	 * @param {EC2.Volume} volume
	 * @param {EC2.Snapshot} snapshot
	 *
	 * @returns {Promise}
	 */
	_validateTemporaryVolume(volume, snapshot) {
		const
			logger = Logger.get(snapshot.SnapshotId),
			
			// The volume may or may not be GPG-encrypted on S3, so check for both:
			ddKeyPlaintext = createS3KeyForSnapshotVolumeImage(snapshot, true, false),
			ddKeyEncrypted = createS3KeyForSnapshotVolumeImage(snapshot, true, true);
		
		let
			isEncrypted = false;
		
		// Check if we have a dd image in S3 to validate against
		return this.s3.headObject({
			Bucket: this.options.bucket,
			Key: ddKeyPlaintext
		}).promise()
			.catch(err => {
				// Okay, look for an encrypted version instead...
				isEncrypted = true;
				
				return this.s3.headObject({
					Bucket: this.options.bucket,
					Key: ddKeyEncrypted
				}).promise()
			})
			.catch(err => {
				// No DD images found, assume it's a tar image
				return null;
			})
			.then(ddObjectHead => {
				logger.info("Waiting for " + volume.VolumeId + "'s partitions to become visible to the operating system...");
				
				return awsTools.waitForVolumePartitions(volume, this.instanceIdentity.instanceId, PARTITION_POLL_MAX_RETRY, PARTITION_POLL_INTERVAL)
					.then(partitions => {
						if (ddObjectHead) {
							return this._validatePartitionsUsingDd(partitions, snapshot, isEncrypted ? ddKeyEncrypted : ddKeyPlaintext, ddObjectHead.ContentLength, isEncrypted, logger);
						} else {
							return this._validatePartitionsUsingTar(partitions, snapshot, logger);
						}
					});
			});
	}
	
	_cleanUpTempVolume(volume, snapshot) {
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
	}
	
	/**
	 *
	 * @param {EC2.Snapshot} snapshot
	 * @returns {Promise}
	 */
	_migrateSnapshot(snapshot) {
		return this._findOrCreateVolumeFromSnapshot(snapshot)
			.then(createdVolume => this._findOrAttachVolumeToInstance(createdVolume, snapshot))
			.then(attachedVolume =>
				this._uploadTemporaryVolume(attachedVolume, snapshot)
					.then(() => this._cleanUpTempVolume(attachedVolume, snapshot))
			);
	}
	
	/**
	 *
	 * @param {EC2.Snapshot} snapshot
	 * @returns {Promise}
	 */
	_validateSnapshot(snapshot) {
		return this._findOrCreateVolumeFromSnapshot(snapshot)
			.then(createdVolume => this._findOrAttachVolumeToInstance(createdVolume, snapshot))
			.then(attachedVolume =>
				this._validateTemporaryVolume(attachedVolume, snapshot)
					.then(() => this._cleanUpTempVolume(attachedVolume, snapshot))
			);
	}
	
	/**
	 *
	 * @param {EC2.Snapshot} snapshot
	 *
	 * @returns {Promise}
	 */
	_claimAndMigrateSnapshot(snapshot) {
		let
			logger = Logger.get(snapshot.SnapshotId);
		
		logger.info("Migrating " + snapshot.SnapshotId + " to S3");
		logger.info("Tagging snapshot with \"migrating\"...");
		
		return this._raceToMarkSnapshot(snapshot.SnapshotId, "migrating")
			.then(() =>
				this._migrateSnapshot(snapshot)
					.then(
						// If we succeeded, mark the snapshot so we don't try to migrate it again
						() => {
							let
								tagWith = this.options.validate ? "validated" : "migrated";
							
							logger.info("Tagging snapshot with \"" + tagWith + "\"");
							return this._markSnapshotAsCompleted(snapshot.SnapshotId, tagWith);
						},
						// If we messed up, mark the snapshot for retry and rethrow the error up the stack
						(err) => {
							logger.info("An error occurred, tagging snapshot with \"migrate\" so it can be retried later");
							return this._markSnapshotForMigration(snapshot.SnapshotId).then(() => {
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
	}
	
	/**
	 *
	 * @param {EC2.Snapshot} snapshot
	 *
	 * @returns {Promise}
	 */
	_claimAndValidateSnapshot(snapshot) {
		let
			logger = Logger.get(snapshot.SnapshotId),
			originalTag;
		
		logger.info("Validating S3 against the original " + snapshot.SnapshotId);
		logger.info("Tagging snapshot with \"validating\"...");
		
		originalTag = snapshot.Tags.find(tag => tag.Key === this.options.tag);
		
		return this._raceToMarkSnapshot(snapshot.SnapshotId, "validating")
			.then(
				() => this._validateSnapshot(snapshot).then(
					// If we succeeded, mark the snapshot so we don't try to validate it again
					() => {
						logger.info("Tagging snapshot with \"validated\"");
						return this._markSnapshotAsCompleted(snapshot.SnapshotId, "validated");
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
								restoreTag = this._markSnapshotAsCompleted(snapshot.SnapshotId, "migrated");
							} else {
								logger.info("An error occurred, restoring the original \"" + originalTag.Value + "\" tag on the snapshot");
								restoreTag = this._markSnapshotAsCompleted(snapshot.SnapshotId, originalTag.Value);
							}
						} else {
							// We might have manually told snap-to-s3 to validate a snapshot that didn't have any tag previously
							logger.info("An error occurred, clearing the \"validating\" tag from the snapshot");
							restoreTag = this._markSnapshotAsCompleted(snapshot.SnapshotId, "");
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
	}
	
	/**
	 * Migrate the snapshots with the given snapshot descriptions.
	 *
	 * The migration is terminated at the first snapshot that fails to migrate.
	 *
	 * @param {EC2.Snapshot[]} snapshots
	 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
	 */
	migrateSnapshotsByDescription(snapshots) {
		let
			promise = this.initPromise;
		
		for (let snapshot of snapshots) {
			promise = promise.then(() => this._claimAndMigrateSnapshot(snapshot));
		}
		
		return promise.then(() => snapshots.map(snapshot => snapshot.SnapshotId));
	}
	
	/**
	 * Find all snapshots that are eligible for migration, copy them over to S3 and mark them as migrated.
	 *
	 * The migration is terminated at the first snapshot that fails to migrate.
	 *
	 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
	 */
	migrateAllTaggedSnapshots() {
		return this.initPromise.then(() => {
			let
				migratedSnapshotIDs = [];
			
			const
				migrateOne = () => this._findMigratableSnapshots().then(snapshots => {
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
	}
	
	/**
	 * Find one snapshot that is eligible for migration, copy it over to S3 and mark it as migrated.
	 *
	 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
	 */
	migrateOneTaggedSnapshot() {
		return this.initPromise
			.then(() => this._findMigratableSnapshots())
			.then(snapshots => {
				if (snapshots.length === 0) {
					return [];
				}
				
				return this.migrateSnapshotsByDescription([snapshots[0]]);
			});
	}
	
	/**
	 * Migrate the specified snapshots to S3
	 *
	 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
	 */
	migrateSnapshots(snapshotIDs) {
		return this.initPromise
			.then(() => describeSnapshots(this.ec2, snapshotIDs))
			.then(snapshots => this.migrateSnapshotsByDescription(snapshots));
	}
	
	/**
	 * @param {EC2.Snapshot[]} snapshots
	 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
	 */
	validateSnapshotsByDescription(snapshots) {
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
	}
	
	/**
	 * Find all snapshots that are eligible for validation, validate them against S3, and mark them as validated if successful.
	 *
	 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
	 */
	validateAllTaggedSnapshots() {
		return this.initPromise.then(() => {
			let
				validatedSnapshotIDs = [];
			
			const
				validateOne = () => this._findValidatableSnapshots().then(snapshots => {
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
	}
	
	/**
	 * Find one snapshot that is eligible for validation, validate it against S3, and mark as validated.
	 *
	 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
	 */
	validateOneTaggedSnapshot() {
		return this.initPromise
			.then(() => this._findValidatableSnapshots())
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
	}
	
	/**
	 * Validate the specified snapshots against S3, and mark them as validated if successful.
	 *
	 * @param {String[]} snapshotIDs
	 * @returns {Promise.<string[]>} - Array of successful snapshot ids.
	 */
	validateSnapshots(snapshotIDs) {
		if (snapshotIDs.length === 0) {
			throw "No snapshot IDs were provided to validate!";
		}
		
		return this.initPromise
			.then(() => describeSnapshots(this.ec2, snapshotIDs))
			.then(snapshots => this.validateSnapshotsByDescription(snapshots));
	}
}


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