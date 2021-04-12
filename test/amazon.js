"use strict";

const
	fs = require("fs"),
	path = require("path"),
	child_process = require("child_process"),
	assert = require("assert"),
	
	Logger = require("js-logger"),
	tar = require("tar-stream"),
	AWS = require("aws-sdk"),
	commandLineArgs = require("command-line-args"),
	getUsage = require("command-line-usage"),
	
	fsTools = require("../lib/filesystem-tools"),
	awsTools = require("../lib/aws-tools"),
	hashFiles = require("../lib/hash-files"),
	SnapToS3 = require("../lib/snap-to-s3"),

	common = require("./common");

const
	// Test EC2 resources will be created with this tag
	TEST_TAG_KEY = "snap-to-s3-test",
	
	PARTITION_POLL_INTERVAL = 4 * 1000, // ms
	PARTITION_POLL_MAX_RETRY = 60,
	
	metadataService = new AWS.MetadataService(),
	
	programOptions = [
		{
			name: "help",
			type: Boolean,
			description: "Show this page"
		},
		{
			name: "agree",
			type: Boolean,
			defaultValue: false
		},
		{
			name: "bucket",
			type: String,
			required: true,
			typeLabel: "{underline name}",
			description: "S3 bucket to upload to for testing (required, must be dedicated for testing only)"
		},
		{
			name: "mount-point",
			type: String,
			required: true,
			defaultValue: "/mnt",
			typeLabel: "{underline path}",
			description: "Temporary volumes will be mounted here, created if it doesn't already exist (default: $default)"
		}
	],
	
	usageSections = [
		{
			header: "Amazon tests for snap-to-s3",
			content: "Constructs a variety of test snapshots and tests snap-to-s3's ability to migrate them to S3. Must " +
			"be run on an EC2 instance.\n\n" +
			"This will incur charges on your Amazon account for test resources, and will likely leave these resources " +
			"behind at the end of the test. Also, there are limited tests for the tests themselves, so you can expect " +
			"a high chance of entirely destroying the filesystem on this instance. This tool does various scary things " +
			"including rm -rf, fdisk and mkfs. Do not run this on an instance that contains any useful data on it. Pass " +
			"--agree if you agree to this."
		},
		{
			header: "Usage",
			content: "npm run test-amazon -- --bucket bucketname"
		},
		{
			header: "Options",
			optionList: programOptions,
			hide: ["agree"]
		}
	];

let
	options,
	
	instanceIdentity = null,
	ec2 = null,
	s3 = null;

class OptionsError extends Error {
	constructor(message) {
		super(message);
	}
}

function initAWS() {
	return new Promise((resolve, reject) => {
		metadataService.fetchMetadataToken(function (err, token) {
			if (err) {
				reject(err);
			} else {
				metadataService.request("/latest/dynamic/instance-identity/document", {
					headers: {"x-aws-ec2-metadata-token": token}
				}, (err, data) => {
					if (err) {
						reject(err);
					} else {
						instanceIdentity = JSON.parse(data);
						
						AWS.config.update({
							region: instanceIdentity.region
						});
						
						s3 = new AWS.S3();
						ec2 = new AWS.EC2();
						
						resolve();
					}
				});
			}
		});
	});
}

/**
 * We use MD5 hashes of tars to uniquely identify them when they're used as part of testcases uploaded to AWS.
 * Compute those .tar.md5 files for any tars that don't have them already, and add .tarHash properties to all
 * testcases.
 *
 * @returns {Promise}
 */
function hashTestTars() {
	return Promise.all(
		common.backupTests.map(test => new Promise((resolve, reject) => {
			let
				tarMD5Filename = test.tarFilename + ".md5";
			
			fs.readFile(tarMD5Filename, { encoding: "utf8" }, (error, data) => {
				if (error || data.length !== 32) {
					common.openSSLMD5File(test.tarFilename)
						.then(
							hash => {
								test.tarHash = hash;
								
								fs.writeFile(tarMD5Filename, hash, { encoding: "utf8" }, error => {
									if (error) {
										reject(error);
									} else {
										resolve();
									}
								});
							},
							error => reject(error)
						);
				} else {
					test.tarHash = data;
					resolve();
				}
			});
		}))
	);
}

/**
 * @param {EC2.Volume} volume
 *
 * @return {Promise.<EC2.Volume>}
 */
function attachVolumeToInstance(volume) {
	return awsTools.pickAvailableAttachmentPoint(ec2, instanceIdentity.instanceId).then(attachPoint => {
		console.error("Attaching " + volume.VolumeId + " to this instance (" + instanceIdentity.instanceId + ") at " + attachPoint);
		
		return ec2.attachVolume({
			Device: attachPoint,
			InstanceId: instanceIdentity.instanceId,
			VolumeId: volume.VolumeId
		}).promise()
		// Although just because the volume is in-use, doesn't mean it will appear in the Attached state to the instance
			.then(() => awsTools.waitForVolumeState(ec2, volume.VolumeId, "in-use", 60, 10 * 1000));
	});
}

/**
 *
 * @param {int} sizeInGB
 * @returns {Promise.<EC2.Volume>}
 */
function createAndAttachTemporaryVolume(sizeInGB) {
	return ec2.createVolume({
		AvailabilityZone: instanceIdentity.availabilityZone,
		VolumeType: "standard",
		Size: sizeInGB,
		TagSpecifications: [
			{
				ResourceType: "volume",
				Tags: [
					{
						Key: "Name",
						Value: "Temp for snap-to-s3 test"
					},
					{
						Key: TEST_TAG_KEY,
						Value: ""
					}
				]
			}
		]
	}).promise()
		.then(volume => awsTools.waitForVolumeState(ec2, volume.VolumeId, "available", 60, 10 * 1000))
		.then(attachVolumeToInstance);
}

/**
 * @param {BlockDevice} device
 * @param {string} mountPoint
 */
function createMountpointAndMount(device, mountPoint) {
	return fsTools.forcePath(mountPoint)
		.then(() => {
			if (device.MOUNTPOINT !== mountPoint) {
				return fsTools.verifyDirectoryEmpty(mountPoint).then(
					() => fsTools.mountPartition(device.DEVICEPATH, device.FSTYPE, mountPoint, false),
					(err) => {
						throw "Mountpoint " + mountPoint + " is not empty!";
					}
				);
			}
		})
}

/**
 *
 * @param {string} mountPoint
 */
function unmountAndRemoveMountPoint(mountPoint) {
	return fsTools.unmount(mountPoint)
		.then(() => {
			fs.rmdirSync(mountPoint);
		});
}

/**
 *
 * @param {EC2.Volume} volume
 * @returns {Promise}
 */
function deleteTemporaryVolume(volume) {
	console.log("Detaching temporary volume " + volume.VolumeId + "...");
	
	return ec2.detachVolume({
		VolumeId: volume.VolumeId
	}).promise()
		.then(() => awsTools.waitForVolumeState(ec2, volume.VolumeId, "available", 60, 10 * 1000))
		.then(() => {
			console.log("Deleting temporary volume " + volume.VolumeId + "...");
			
			return ec2.deleteVolume({
				VolumeId: volume.VolumeId
			}).promise();
		});
}

function runSfdisk(devicePath, script, extraArgs) {
	return new Promise((resolve, reject) => {
		let
			sfdisk, errorBuffer, args = [];
		
		if (extraArgs) {
			args = args.concat(extraArgs);
		}
		args.push(devicePath);
		
		sfdisk = child_process.spawn("sfdisk", args, {
			stdio: ["pipe", "pipe", "pipe"]
		});
		
		sfdisk.on("close", code => {
			if (code !== 0) {
				reject("Failed to run \"sfdisk " + args.join(" ") + "\", was using this script:\n" + script + "\n\n" + errorBuffer);
			} else {
				resolve();
			}
		});
		
		sfdisk.stderr.on("data", data => {
			errorBuffer += data;
		});
		
		sfdisk.stdout.on("data", data => {
			errorBuffer += data;
		});
		
		sfdisk.stdin.write(script);
		sfdisk.stdin.end();
	});
}

/**
 * Divide a disk up evenly into a given number of partitions.
 *
 * @param {BlockDevice} device
 * @param {int} numPartitions
 *
 * @returns Promise.<BlockDevice[]>
 */
function divideDiskIntoPartitions(device, numPartitions) {
	// Check for GPT support in sfdisk (new in util-linux 2.26)
	return runSfdisk(device.DEVICEPATH, "label:gpt", ["-n"])
		.then(
			() => true,
			err => false
		)
		.then(supportsGPT => {
			const
				SECTOR_ALIGNMENT = 2048, // i.e. 1MB for 512 byte sectors
				FIRST_SECTOR = 2048,
				SECTOR_SIZE = device["LOG-SEC"],
				
				PART_SIZE_SECTORS = Math.floor(Math.floor((device.SIZE / SECTOR_SIZE - FIRST_SECTOR) / numPartitions) / SECTOR_ALIGNMENT) * SECTOR_ALIGNMENT;
			
			let
				currentSector = FIRST_SECTOR,
				sfdiskScript = "unit: sectors\n",
				extraSFArgs = [];
			
			if (supportsGPT) {
				sfdiskScript += "label: gpt\n";
			} else {
				// To add more than 4 partitions we'd need support for adding an extended partition to hold them:
				assert(numPartitions <= 4);
				// Thanks very much https://bugs.launchpad.net/ubuntu/+source/util-linux/+bug/1481158 :
				extraSFArgs = ["--force"];
			}
			
			sfdiskScript += "\n";
			
			for (let i = 0; i < numPartitions; i++) {
				if ((currentSector % SECTOR_ALIGNMENT) !== 0) {
					currentSector += SECTOR_ALIGNMENT - (currentSector % SECTOR_ALIGNMENT);
				}
				
				if (supportsGPT) {
					sfdiskScript += "start=" + currentSector;
					
					// Final partition takes the remainder of the disk space
					if (i < numPartitions - 1) {
						sfdiskScript += ",size=" + PART_SIZE_SECTORS;
					}
					
					if (supportsGPT) {
						sfdiskScript += ',type=0FC63DAF-8483-4772-8E79-3D69D8477DE4, name="Linux filesystem"';
					} else {
						sfdiskScript += ',Id=83';
					}
					
					sfdiskScript += '\n';
				} else {
					sfdiskScript += currentSector + ",";
					
					// Due to bug https://bugs.launchpad.net/ubuntu/+source/util-linux/+bug/1481158
					// we can't have old sfdisk calculate the size of the final partition for us.
					sfdiskScript += PART_SIZE_SECTORS;
					
					sfdiskScript += ",L,-\n";
				}
				
				currentSector += PART_SIZE_SECTORS;
			}
			
			return runSfdisk(device.DEVICEPATH, sfdiskScript, extraSFArgs);
		});
}

/**
 * The given temporary volume is attached to this instance, partition it as needed and write the given tests there.
 *
 * @param {EC2.Volume} volume
 * @param {Test[]|Test} tests
 *
 * @returns {Promise.<BlockDevice>} - The disk that was filled with data
 */
function fillTestVolumeWithTests(volume, tests) {
	const
		usingPartitionTable = Array.isArray(tests);
	
	if (!Array.isArray(tests)) {
		tests = [tests];
	}
	
	return awsTools.waitForVolumePartitions(volume, instanceIdentity.instanceId, PARTITION_POLL_MAX_RETRY, PARTITION_POLL_INTERVAL)
		.then(/** @type {BlockDevice[]} */ partitions => {
			// Partition the disk
			assert(partitions.length === 1 && partitions[0].TYPE === "disk");
			
			const
				disk = partitions[0];
			
			// We expect the disk to be blank before we start!
			assert(disk.FSTYPE === "");
			assert(disk.MOUNTPOINT === "");
			assert(disk.SIZE > 0);
			
			if (usingPartitionTable) {
				console.log("Dividing disk into " + tests.length + " partitions of equal size...");
				
				return divideDiskIntoPartitions(disk, tests.length)
					.then(() => new Promise(resolve => {
						// Give the OS some time to finish creating and publishing those partitions for us...
						setTimeout(resolve, 4000);
					}))
					.then(() => awsTools.identifyPartitionsForAttachedVolume(volume, instanceIdentity.instanceId));
			} else {
				return partitions;
			}
		})
		.then(partitions => {
			// Create filesystems and unpack tars into each partition
			let
				wholeDisk = partitions.find(partition => partition.TYPE === "disk");
			
			partitions = partitions.filter(partition => partition.TYPE === (usingPartitionTable ? "part" : "disk"));
			
			assert(partitions.length  === tests.length);
			assert(wholeDisk);
			
			let
				promise = Promise.resolve();
			
			tests.forEach((test, testIndex) => {
				let
					partition = partitions[testIndex];
				
				promise = promise
					.then(() => new Promise((resolve, reject) => {
						console.log("Creating ext3 filesystem on " + partition.DEVICEPATH + "...");
						
						child_process.execFile("mkfs.ext3", [partition.DEVICEPATH], (error, stdout, stderr) => {
							if (error) {
								reject("Failed creating ext3 filesystem on " + partition.DEVICEPATH + ": " + error + " " + stderr);
							} else {
								resolve();
							}
						});
					}));
			});
			
			promise = promise
				// After creating the filesystems, refresh the partition list to get new filesystem information
				.then(() => awsTools.identifyPartitionsForAttachedVolume(volume, instanceIdentity.instanceId))
				.then(partitions => {
					let
						promise = Promise.resolve();
					
					partitions = partitions.filter(partition => partition.TYPE === (usingPartitionTable ? "part" : "disk"));
					
					tests.forEach((test, testIndex) => {
						let
							partition = partitions[testIndex],
							mountPath = options["mount-point"] + volume.VolumeId + "-" + testIndex;
						
						promise = promise
							.then(() => {
								console.log("Mounting " + partition.DEVICEPATH + " to " + mountPath);
								return createMountpointAndMount(partition, mountPath);
							})
							.then(() => {
								console.log("Writing tar from test '" + test.name + "' to " + mountPath);
								return common.extractTarToDirectory(test.tarFilename, mountPath)
							})
							.then(() => {
								console.log("Unmounting " + mountPath);
								return unmountAndRemoveMountPoint(mountPath);
							});
					});
					
					return promise;
				});

			return promise.then(() => wholeDisk);
		});
}

/**
 * Find an existing, or create a new, test snapshot whose partitions are created from the tars specified by the given tests.
 * If 'tests' is an array, create a partition table, otherwise create one filesystem on the entire disk.
 *
 * @param {Test[]|Test} tests
 * @param {int} driveSizeGB
 *
 * @returns {Promise.<EC2.Snapshot>}
 */
function findOrCreateTestSnapshot(tests, driveSizeGB) {
	let
		testDisplayName = Array.isArray(tests) ? "[" + tests.map(test => test.name).join(", ") + "]" : tests.name,
		targetSnapshotDescription = Array.isArray(tests) ? "Parts: " + tests.map(test => test.tarHash).join(",") : "Disk: " + tests.tarHash;
	
	console.log("Looking for snapshot to use for test with partitions: " + testDisplayName);
	
	return ec2.describeSnapshots({
		Filters: [
			{
				Name: "status",
				Values: [
					"completed"
				]
			},
			{
				Name: "tag-key",
				Values: [
					TEST_TAG_KEY
				]
			},
			{
				Name: "description",
				Values: [
					targetSnapshotDescription
				]
			}
		],
		OwnerIds: [
			instanceIdentity.accountId
		]
	}).promise().then(data => {
		for (let /** @type {EC2.Snapshot} */ snapshot of data.Snapshots) {
			assert(snapshot.Description === targetSnapshotDescription);
			assert(snapshot.Tags.find(tag => tag.Key === TEST_TAG_KEY));
			
			console.log("Using existing snapshot " + snapshot.SnapshotId);
			return snapshot;
		}
		
		console.log("Snapshot doesn't exist yet, so building a temporary volume for this test...");
		console.log("Creating temporary EBS volume of size " + driveSizeGB + "GB...");
		
		return createAndAttachTemporaryVolume(driveSizeGB)
			.then(volume =>
				fillTestVolumeWithTests(volume, tests)
					.then(diskDevice => {
						console.log("Finished building temp volume, hashing it with MD5...");
	
						return common.openSSLMD5File(diskDevice.DEVICEPATH);
					})
					.then(volumeHash => {
						console.log("Whole volume hashes to " + volumeHash);
						console.log("Taking snapshot of it...");
						
						return ec2.createSnapshot({
							Description: targetSnapshotDescription,
							VolumeId: volume.VolumeId,
						}).promise()
							.then(
								snapshot => ec2.waitFor("snapshotCompleted", {
									SnapshotIds: [snapshot.SnapshotId]
								}).promise()
							)
							.then(data => {
								let
									snapshot = data.Snapshots[0],
									newTags = [
										{
											Key: TEST_TAG_KEY,
											Value: ""
										},
										{
											Key: TEST_TAG_KEY + "-md5",
											Value: volumeHash
										},
										{
											Key: "Name",
											Value: "snap-to-s3 testcase"
										}
									];
								
								return ec2.createTags({
									Resources: [
										snapshot.SnapshotId
									],
									Tags: newTags
								}).promise()
									.then(() => deleteTemporaryVolume(volume))
									.then(() => {
										/* The tags we just created aren't part of the snapshot description we fetched earlier.
										 * Just add those in to our local copy instead of waiting for AWS to reflect our
										 * changes.
										 */
										snapshot.Tags = newTags;
										
										return snapshot;
									});
							});
					})
			);
	});
}

/**
 * Create a snapshot of a volume that contains the given tests, then see if snap-to-s3 can migrate the snapshot
 * faithfully.
 *
 * @param {Test|Test[]} tests - A series of tests which will be added as separate partitions to one volume for snapshotting.
 * @param {int} driveSizeGB
 * @param {boolean} useDD - Use DD disk imaging to migrate the snapshot rather than tar.
 * @returns {Promise}
 */
function testSnapshotMigration(tests, driveSizeGB, useDD) {
	return findOrCreateTestSnapshot(tests, driveSizeGB)
		.then(snapshot => {
			if (!Array.isArray(tests)) {
				tests = [tests];
			}
			
			let
				snapToS3 = new SnapToS3({
					bucket: options.bucket,
					"compression-level": 1,
					"mount-point": options["mount-point"],
					"volume-type": "standard",
					"tag": "snap-to-s3-test",
					"validate": true,
					"dd": !!useDD // Turn undefined into a nice clean false
				}),
				testIndex,
			
				checkHashReceivedCount = () => {
					if (useDD) {
						if (testIndex !== 1) {
							throw "Didn't receive a confirmation of the MD5 hash for the device from snap-to-s3, what went wrong?";
						}
					} else if (testIndex !== tests.length) {
						throw "Didn't receive file hashes for every partition we supplied, did they all get uploaded?";
					}
				};

			snapToS3.debugHandler = event => {
				switch (event.event) {
					case "tarHashesVerified":
						let
							test = tests[testIndex++];
						
						return hashFiles.compareHashListFiles(
							event.remoteHashesFile, "uploaded tar",
							test.tarHashesFilename, "test expectation"
						).then(numberMatched => {
							assert(numberMatched === test.numNormalFiles);
						});
					break;
					case "ddHashVerified":
						// We attached a hash of the entire test volume snapshot when we first created it, use that
						let
							expected = snapshot.Tags.find(tag => tag.Key === TEST_TAG_KEY + "-md5").Value;
						
						assert(expected.length === 32);
						assert(expected === event.hash);
						testIndex++;
					break;
				}
			};
			
			console.log("Attempting to migrate " + snapshot.SnapshotId + " using snap-to-s3 in " + (useDD ? "--dd " : "") + "--migrate --validate mode:");
			
			testIndex = 0;
			return snapToS3.migrateSnapshots([snapshot.SnapshotId])
				.then(() => {
					checkHashReceivedCount();
					
					console.log("Test passed: Migrated + validated snapshot successfully");
					console.log("Attempting to validate " + snapshot.SnapshotId + " using snap-to-s3 in --validate mode:");
					
					testIndex = 0;
					return snapToS3.validateSnapshots([snapshot.SnapshotId]);
				})
				.then(() => {
					checkHashReceivedCount();
					
					console.log("Test passed: Validated snapshot successfully");
				});
		});
}

function remountReadWrite(devicePath, mountPoint) {
	return new Promise(function (resolve, reject) {
		let
			args = ["--source", devicePath, "--target", mountPoint, "-o", "remount,rw"];
		
		child_process.execFile("mount", args, function (error, stdout, stderr) {
			if (error) {
				reject("mount " + args.join(" ") + " failed: " + stdout + " " + stderr);
			} else {
				resolve();
			}
		});
	});
}

function testSnapshotCorruption() {
	let
		test = common.backupTests.find(test => test.name === "links");
	
	return findOrCreateTestSnapshot(test, 1)
		.then(snapshot => {
			let
				snapToS3 = new SnapToS3({
					bucket: options.bucket,
					"compression-level": 1,
					"mount-point": options["mount-point"],
					"volume-type": "standard",
					"tag": "snap-to-s3-test",
					"validate": false,
					"keep-temp-volumes": true
				}),
				tempPartition = null;
			
			snapToS3.debugHandler = (event) => {
				switch (event.event) {
					case "temporaryPartitionMounted":
						if (tempPartition === null) {
							tempPartition = event;
						}
				}
			};
			
			console.log("Migrating " + snapshot.SnapshotId + " using snap-to-s3 in --migrate mode:");
			
			return snapToS3.migrateSnapshots([snapshot.SnapshotId])
				.then(() => {
					assert(tempPartition);
					
					console.log("Now damaging a file on our local copy and checking again...");
					
					return remountReadWrite(tempPartition.devicePath, tempPartition.mountPoint);
				})
				.then(() => {
					let
						mountPoint = tempPartition.mountPoint,
						targetFilename;
					
					if (!mountPoint.match(/\/$/)) {
						mountPoint = mountPoint + "/";
					}
					
					targetFilename = mountPoint + "test-a.txt";
					
					console.log("Adding an extra character to the end of " + targetFilename + " to corrupt it");
					
					fs.statSync(targetFilename);
					fs.appendFileSync(targetFilename, "!");
					
					/* Now our "pristine copy of the EBS snapshot" embodied in our attached EBS volume will differ from
					 * the tar we uploaded previously. This should trigger an error upon verification:
					 */
					return snapToS3.validateSnapshots([snapshot.SnapshotId]);
				})
				.then(
					() => {
						throw "snap-to-s3 failed to notice the corrupted file!";
					},
					err => {
						let
							errorMessage = "" + err;
						assert(errorMessage.match(/e0e7398575d117f63c4ecab9f685f2ba/) && errorMessage.match(/test-a\.txt/) && errorMessage.match(/test-hardlink\.txt/));
						
						console.log("snap-to-s3 failed (as we hoped)!");
					}
				);
		});
}

function runTests() {
	return initAWS()
		.then(() => {
			console.log("Generating tars for tests...");
			return common.createTestTars();
		})
		.then(() => {
			console.log("Hashing test tars...");
			return hashTestTars();
		})
		.then(() => {
			console.log("");
			
			let
				promise = Promise.resolve(),
				testsByName = {};
			
			for (let test of common.backupTests) {
				testsByName[test.name] = test;
			}
			
			for (let test of common.backupTests) {
				promise = promise
					.then(() => {
						console.log("=== Running test with no partition table: " + test.name + " ===");
						
						return testSnapshotMigration(test, 1, false);
					})
					.then(() => {
						console.log("");
					});
			}
			
			promise = promise
				.then(() => {
					console.log("=== Testing multi-partition handling using tar ===");
					
					return testSnapshotMigration([testsByName["big-random"], testsByName["links"], testsByName["nested-directories"]], 3, false);
				})
				.then(() => {
					console.log("");
					console.log("=== Testing multi-partition handling using dd ===");
					
					/* Here we choose a configuration of tests not used before in tar mode, since snap-to-s3 will always
					 * prefer to --validate against the dd version we're about to upload, spoiling later tar tests for us
					 */
					return testSnapshotMigration([testsByName["big-random"], testsByName["links"]], 2, true);
				});
			
			promise = promise
				.then(() => {
					console.log("=== Checking that --verify is able to detect corrupted files ===");
					
					return testSnapshotCorruption();
				});
			
	
			return promise;
		});
}

Logger.useDefaults();

for (let option of programOptions) {
	if (option.description) {
		option.description = option.description.replace("$default", option.defaultValue);
	}
}

try {
	// Parse command-line options
	options = commandLineArgs(programOptions);
	
	if (!options["mount-point"].match(/\/$/)) {
		options["mount-point"] = options["mount-point"] + "/";
	}
	
	if (options["mount-point"] === "/") {
		throw "Mount point must not be empty, or /";
	}
} catch (e) {
	console.error("Error: " + e.message);
	options = null;
}

if (options === null || options.help || !options.agree || process.argv.length <= 2) {
	console.log(getUsage(usageSections));
} else {
	Promise.resolve().then(() => {
		for (let option of programOptions) {
			if (option.required) {
				if (options[option.name] === undefined) {
					throw new OptionsError("Option --" + option.name + " is required!");
				} else if (options[option.name] === null) {
					throw new OptionsError("Option --" + option.name + " requires an argument!");
				}
			}
		}
		
		return runTests().then(() => { console.log("Done!"); });
	})
	.catch(err => {
		process.exitCode = 1;
		
		if (err instanceof OptionsError) {
			console.error(err.message);
		} else {
			console.error("Error: " + err + " " + (err.stack ? err.stack : ""));
			console.error("");
			console.error("Terminating due to fatal errors.");
		}
	});
}
