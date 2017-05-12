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
 * @returns {Promise}
 */
module.exports.mountPartition = function(device, filesystem, mountPoint) {
	return new Promise(function(resolve, reject) {
		let
			args = ["--source", device, "--read-only", "--target", mountPoint];
		
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
 * @property {int} SIZE - In bytes
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

/**
 * Returns an array of one object per block device returned from lsblk, with keys NAME, FSTYPE, MOUNTPOINT, SIZE and TYPE
 *
 * @returns {Promise.<BlockDevice[]>}
 */
module.exports.listBlockDevices = function() {
	// Check for PKNAME support, since it's pretty new
	return supportsPKNAME().then((supportsPKNAME) => new Promise(function (resolve, reject) {
		let
			columns = "NAME,FSTYPE,MOUNTPOINT,SIZE,TYPE";
		
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
						parameters = line.match(/(^|\s)[A-Z]+="[^"\n]*"/g);
					
					if (parameters) {
						let
							device = {};
						
						for (let parameter of parameters) {
							let
								matches = parameter.match(/^\s*([A-Z]+)="([^"\n]*)"$/);
							
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
	}));
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