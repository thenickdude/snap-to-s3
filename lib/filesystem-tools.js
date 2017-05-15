"use strict";

const
	child_process = require("child_process"),
	mkdirp = require('mkdirp'),
	fs = require("fs"),
	crypto = require("crypto"),
	assert = require("assert"),
	
	tar = require("tar-stream"),
	
	SimpleProgressStream = require("./simple-progress-stream");

/**
 * Calls the callback for every regular file in the given path.
 *
 * @param {String} rootDirectory
 * @param {function} fileCallback - Called with (filename, fileStat, next), call next() with an error or null to continue or abort.
 * @returns {Promise}
 */
module.exports.recurseRegularFilesInDirectory = function(rootDirectory, fileCallback) {
	return new Promise(function(resolve, reject) {
		const
			processDirectory = function(directory, afterDirectory) {
				if (!directory.match(/\/$/)) {
					directory = directory + "/";
				}
				
				fs.readdir(directory, function(err, files) {
					if (err) {
						afterDirectory(err);
					} else {
						let
							nextFileIndex = 0,
							
							next = function (error) {
								if (nextFileIndex >= files.length || error) {
									afterDirectory(error);
								} else {
									const
										filename = directory + files[nextFileIndex++],
										stat = fs.lstatSync(filename);
									
									if (!stat.isSymbolicLink() && stat.isFile()) {
										fileCallback(filename, stat, next);
									} else if (!stat.isSymbolicLink() && stat.isDirectory()) {
										processDirectory(filename, next);
									} else {
										next(null);
									}
								}
							};
						
						next(null);
					}
				});
			};
		
		processDirectory(rootDirectory, function(err) {
			if (err) {
				reject(err);
			} else {
				resolve();
			}
		});
	});
};

function capitaliseFirstLetter(text) {
	if (text.length > 0) {
		text = text.substring(0, 1).toUpperCase() + text.substring(1);
	}
	
	return text;
}

/**
 *
 * @param {Object} tree1
 * @param {string} tree1Name
 * @param {Object} tree2
 * @param {string} tree2Name
 *
 * @returns string[] - Array of messages which explain differences, or an empty array if both trees are equal.
 */
module.exports.compareFileTreeHashes = function(tree1, tree1Name, tree2, tree2Name) {
	let
		errors = [],
		tree1Count = 0,
		tree2Count = 0;
	
	assert(tree1 && tree2);
	
	for (let filename in tree1) {
		if (tree1.hasOwnProperty(filename)) {
			if (filename in tree2) {
				if (tree1[filename] !== tree2[filename]) {
					errors.push("Difference in file: " + filename + "\n" + capitaliseFirstLetter(tree1Name) + ": " + tree1[filename] + "\n" + capitaliseFirstLetter(tree2Name) + ": " + tree2[filename]);
				}
			} else {
				errors.push("Only in " + tree1Name + ": " + filename);
			}
			tree1Count++;
		}
	}
	
	for (let filename in tree2) {
		if (tree2.hasOwnProperty(filename)) {
			if (!(filename in tree1)) {
				errors.push("Only in " + tree2Name + ": " + filename);
			}
			tree2Count++;
		}
	}
	
	if (tree1Count !== tree2Count) {
		errors.push("File counts differ!");
		errors.push(tree1Name + ": " + tree1Count + " files, " + tree2Name + ": " + tree2Count + " files");
	}
	
	return errors;
};

/**
 * Return an object which maps filenames within the given directory into MD5 hashes of the content of those files.
 *
 * @param {String} directory
 * @param {function} progress
 * @returns {Promise.<Object>}
 */
module.exports.hashFilesInDirectory = function(directory, progress) {
	const
		fileHashes = {},
		// Remove the directory name from the start of the file names
		prefixToRemove = directory.match(/\/$/) ? directory : directory + "/";
	
	return module.exports.recurseRegularFilesInDirectory(directory, (filename, stat, next) => {
		const
			md5 = crypto.createHash("md5"),
			fileStream = fs.createReadStream(filename, {
				highWaterMark: 1024 * 1024
			}),
			streamMeter = new SimpleProgressStream(10 * 1024 * 1024);
		
		let
			failed = false;
		
		streamMeter.on("progress", bytes => {
			progress(bytes);
		});
		
		md5.on('readable', () => {
			const
				data = md5.read();
			
			if (data) {
				let
					hex = data.toString("hex");
				
				assert(hex.length === 32);
				assert(filename.indexOf(prefixToRemove) === 0);
				
				if (!failed) {
					fileHashes[filename.substring(prefixToRemove.length)] = hex;
					next();
				}
			}
		});
		
		for (let stream of [md5, streamMeter, fileStream]) {
			stream.on("error", err => {
				if (!failed) {
					failed = true;
					next(err);
				}
			});
		}
		
		fileStream.pipe(streamMeter).pipe(md5);
	}).then(() => fileHashes);
};

/**
 * Return an object that maps filenames of files from the tar into their MD5 hashes.
 *
 * Non-file entries are ignored (directory, link, device, etc).
 *
 * @param {stream.Readable} stream
 * @returns {Promise.<Object>}
 */
module.exports.hashTarFilesFromStream = function(stream) {
	return new Promise((resolve, reject) => {
		const
			extract = tar.extract(),
			fileHashes = {},
			removeRootPrefix = filename => filename.replace(/^(\.\/|\/)/, "");
		
		extract.on('entry', function(fileHeader, fileStream, next) {
			const
				prefixlessFilename = removeRootPrefix(fileHeader.name);
			
			switch (fileHeader.type) {
				case "file":
					const
						md5 = crypto.createHash("md5");
					
					md5.on('readable', () => {
						const
							data = md5.read();
						
						if (data) {
							let
								hex = data.toString("hex");
							
							assert(hex.length === 32);
							
							fileHashes[prefixlessFilename] = hex;
							
							next();
						}
					});
					
					fileStream.pipe(md5);
				break;
				case "link": //Hard link to a file that was already included in the tar stream
					const
						linkTarget = removeRootPrefix(fileHeader.linkname);
				
					if (!(linkTarget in fileHashes)) {
						reject("Failed to resolve tar hardlink from " + prefixlessFilename + " to " + linkTarget);
						extract.destroy();
					} else {
						// Copy the hash from the linked-to file in the stream
						fileHashes[prefixlessFilename] = fileHashes[linkTarget];
					}
					
					fileStream.on('end', function() {
						next();
					});
					
					fileStream.resume(); // Drain the stream
				break;
				default:
					fileStream.on('end', function() {
						next();
					});

					fileStream.resume(); // Drain the stream
			}
		});
		
		extract.on("finish", () => {
			resolve(fileHashes);
		});
		
		extract.on("error", (err) => reject(err));
		
		stream.pipe(extract);
	});
};

/**
 *
 * @param {String} directory - Directory or file to get size of
 * @returns {Promise.<Number>} Sum of filesizes of all files in the given directory
 */
module.exports.getRecursiveFileSize = function(directory) {
	return new Promise(function(resolve, reject) {
		child_process.execFile("du", ["--bytes", "--summarize", directory], {
			encoding: "utf8"
		}, function (error, stdout) {
			if (error) {
				reject(error);
			} else {
				resolve(parseInt(stdout, 10));
			}
		});
	});
};

module.exports.verifyDirectoryEmpty = function(directory) {
	return new Promise(function(resolve, reject) {
		fs.readdir(directory, function(err, files) {
			if (err) {
				reject(err);
			} else if (files.length > 0) {
				reject("Directory " + directory + " is not empty!");
			} else {
				resolve();
			}
		});
	});
};

/**
 *
 * @param {string} device - Path to device to mount (e.g. /dev/xvda)
 * @param {string} filesystem - Name of filesystem type on device
 * @param {string} mountPoint - Path to mount to
 * @param {boolean} readOnly
 *
 * @returns {Promise}
 */
module.exports.mountPartition = function(device, filesystem, mountPoint, readOnly) {
	return new Promise(function(resolve, reject) {
		let
			args = ["--source", device, "--target", mountPoint];
		
		if (readOnly) {
			args.push("--read-only");
		}
		
		// Allow XFS filesystems with duplicate UUIDs to be concurrently mounted
		if (filesystem === "xfs") {
			args.push("-o");
			args.push("nouuid");
		}
		
		child_process.execFile("mount", args, function(error, stdout, stderr) {
			if (error) {
				reject("mount " + args.join(" ") + " failed: " + stdout + " " + stderr);
			} else {
				resolve(device);
			}
		});
	});
};

module.exports.unmount = function(mountPoint) {
	return new Promise(function(resolve, reject) {
		let
			args = [mountPoint];
		
		child_process.execFile("umount", args, function(error, stdout, stderr) {
			if (error) {
				reject("umount " + args.join(" ") + " failed: " + stdout + " " + stderr);
			} else {
				resolve();
			}
		});
	});
};

/**
 * @typedef {Object} BlockDevice
 * @property {string} NAME - e.g. xvda
 * @property {string} DEVICEPATH - e.g. /dev/xvda
 * @property {string} FSTYPE - e.g. "xfs","ext3",""
 * @property {string} MOUNTPOINT - e.g. /
 * @property {string} PARTNAME - A short unique identifier for this partition within its parent volume (e.g. xvda1 -> "1", xvda -> "")
 * @property {int} SIZE - In bytes of this device
 * @property {int} LOG-SEC - Size in bytes of a logical sector on the device (likely 512 or 4096)
 * @property {int} PHY-SEC - Size in bytes of a physical sector on the device (likely 512 or 4096)
 * @property {string} TYPE - part/disk
 * @property {string} PKNAME - Name of the parent device, or an empty string.
 */

function supportsPKNAME() {
	return new Promise(function(resolve, reject) {
		child_process.execFile("lsblk", ["--output", "PKNAME"], function(error) {
			resolve(error === null);
		});
	});
}

function udevSettle() {
	/* lsblk man page tells us that it depends on udev for fetching some block information (where udev is available),
	 * and if a drive is newly-attached, this information may not be available yet.
	 *
	 * In my own testing, this has shown itself as newly-attached drives appearing in the lsblk output, but missing
	 * fields such as their filesystem information, leading to a failed drive mount later (with mountPartition()).
	 *
	 * Here we wait for udev to complete its task queue, if udevadm is available.
	 */
	return new Promise(function(resolve) {
		// Ignore it if no udevadm on this OS, or it suggested a failure, nothing useful we can do in that case
		child_process.execFile("udevadm", ["settle"], resolve);
	});
}

/**
 * Returns an array of one object per block device returned from lsblk, with keys NAME, FSTYPE, MOUNTPOINT, SIZE and TYPE
 *
 * @returns {Promise.<BlockDevice[]>}
 */
module.exports.listBlockDevices = function() {
	// Check for PKNAME support, since it's pretty new
	return supportsPKNAME().then(supportsPKNAME => udevSettle().then(() => new Promise(function (resolve, reject) {
		let
			columns = "NAME,FSTYPE,MOUNTPOINT,SIZE,TYPE,LOG-SEC,PHY-SEC";
		
		if (supportsPKNAME) {
			columns += ",PKNAME";
		}
		
		child_process.execFile("lsblk", ["--bytes", "--pairs", "--output", columns], function (error, stdout, stderr) {
			if (error) {
				reject("Failed to lsblk " + stdout + " " + stderr);
			} else {
				let
					devices = [];
				
				for (let line of stdout.split("\n")) {
					let
						parameters = line.match(/(^|\s)[A-Z-]+="[^"\n]*"/g);
					
					if (parameters) {
						let
							device = {};
						
						for (let parameter of parameters) {
							let
								matches = parameter.match(/^\s*([A-Z-]+)="([^"\n]*)"$/);
							
							device[matches[1]] = matches[2];
						}
						
						/* If the parent device's name is the prefix of ours, use our suffix as the short name of
						 * this partition
						 */
						if (supportsPKNAME) {
							if (device.PKNAME.length > 0 && device.NAME.indexOf(device.PKNAME) === 0) {
								device.PARTNAME = device.NAME.substring(device.PKNAME.length);
							} else if (device.TYPE === "disk") {
								device.PARTNAME = "";
							} else {
								device.PARTNAME = device.NAME;
							}
						}
						
						device.DEVICEPATH = "/dev/" + device.NAME;
						device.SIZE = parseInt(device.SIZE);
						device["LOG-SEC"] = parseInt(device["LOG-SEC"]);
						device["PHY-SEC"] = parseInt(device["PHY-SEC"]);
						
						devices.push(device);
					}
				}
				
				if (!supportsPKNAME) {
					for (let device of devices) {
						// We could synthesize a PKNAME from our best-guess, but it's probably better not to:
						device.PKNAME = "";
						// Fall back to just using the name of the device if we can't come up with anything shorter
						device.PARTNAME = device.NAME;
						
						switch (device.TYPE) {
							case "part":
								let
									parentDisk = devices.find(parent => parent.TYPE === "disk" && device.NAME.indexOf(parent.NAME) === 0);
								
								if (parentDisk) {
									device.PARTNAME = device.NAME.substring(parentDisk.NAME.length);
								}
								break;
							case "disk":
								device.PARTNAME = "";
								break;
						}
					}
				}
				
				resolve(devices);
			}
		});
	})));
};

/**
 *
 * @param {String} path
 * @returns {Promise}
 */
module.exports.forcePath = function(path) {
	return new Promise(function (resolve, reject) {
		mkdirp(path, function (err) {
			if (err) {
				reject(err)
			} else {
				resolve();
			}
		});
	});
};