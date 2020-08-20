"use strict";

const
	child_process = require("child_process"),
	mkdirp = require('mkdirp'),
	fs = require("fs");

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

/**
 *
 * @param {String} columnName
 * @returns {Promise<boolean>} - True if column is supported
 */
function lsblkSupportsColumn(columnName) {
	return new Promise(function(resolve) {
		child_process.execFile("lsblk", ["--output", columnName], function(error) {
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
	return lsblkSupportsColumn("PKNAME").then(supportsPKNAME => udevSettle().then(() => new Promise(function (resolve, reject) {
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

module.exports.createTempDirectory = function(dirNamePrefix) {
	return new Promise(function(resolve, reject) {
		fs.mkdtemp("/tmp/" + dirNamePrefix, function (err, path) {
			if (err) {
				reject(err);
			} else {
				resolve(path);
			}
		})
	});
};