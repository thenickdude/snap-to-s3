"use strict";

const
	assert = require("assert"),
	crypto = require("crypto"),
	
	deepEqual = require("deep-equal"),

	fsTools = require("./filesystem-tools");

/**
 * Find a list of the partitions associated with the given volume that are visible to our operating system.
 *
 * @param {EC2.Volume} volume
 * @param {String} instanceID
 * @returns {Promise.<BlockDevice[]>}
 */
function identifyPartitionsForAttachedVolume(volume, instanceID) {
	let
		attachment = volume.Attachments.find(attachment => attachment.InstanceId === instanceID);
	
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
	
	// Volume is not even attached here yet.
	return Promise.reject("Called identifyPartitionsForAttachedVolume for a volume that isn't attached here.");
}

/**
 * Wait for the given attached volume to appear as a block device in our OS.
 *
 * @param {EC2.Volume} volume
 * @param {string} instanceID
 * @param {int} maxRetries
 * @param {int} retryInterval - Milliseconds
 *
 * @returns {Promise.<BlockDevice[]>} Resolves to an array of details of the discovered partitions.
 */
function waitForVolumePartitions(volume, instanceID, maxRetries, retryInterval) {
	retryInterval = retryInterval || 4000;
	
	return new Promise(function(resolve, reject) {
		let
			lastSeen = null,
			
			attempt = () => {
				identifyPartitionsForAttachedVolume(volume, instanceID)
					.then(partitions => {
						assert(Array.isArray(partitions));
						
						if (partitions.length === 0) {
							lastSeen = null;
							if (maxRetries <= 0) {
								reject("Timed out waiting for " + volume.VolumeId + " to appear in /dev");
							} else {
								maxRetries--;
								setTimeout(attempt, retryInterval);
							}
						} else if (deepEqual(lastSeen, partitions)) {
							resolve(partitions);
						} else {
							/*
							 * When we see one desired device in the listing, it might be that the OS is still busy
							 * making further partition details for that volume available. (e.g. we might see the disk
							 * block device but no partitions yet, if we were super unlucky). Wait a bit and retry until
							 * the results have settled.
							 */
							lastSeen = partitions;
							
							if (maxRetries <= 0) {
								reject("Timed out waiting for " + volume.VolumeId + "'s partition list to settle");
							} else {
								maxRetries--;
								setTimeout(attempt, retryInterval);
							}
						}
					})
					.catch(reject);
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
		
		assert(availableLetters.length <= 32);
		
		do {
			random = crypto.randomBytes(1)[0] % 32;
		} while (random >= availableLetters.length); // This reject/retry loop avoids creating a bias in the random numbers
		
		return "/dev/sd" + availableLetters[random];
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
				}).promise()
					.then(data => {
						for (let volume of data.Volumes) {
							if (states.indexOf(volume.State) !== -1) {
								return resolve(volume);
							}
						}
						
						if (maxRetries <= 0) {
							reject("Timed out waiting for " + volumeID + " to enter state '" + state + "', currently in state '" + data.Volumes[0].State + "'.");
						} else {
							maxRetries--;
							setTimeout(attempt, retryInterval || 4000);
						}
					})
					.catch(reject);
			};
		
		attempt();
	});
}

/**
 * Wait for the given volume to attach to the given instance. Resolves to a description of the volume.
 *
 * @param {Object} ec2
 * @param {string} volumeID
 * @param {string} instanceID
 * @param {int} maxRetries
 * @param {int} retryInterval - Milliseconds
 * @returns {Promise.<EC2.Volume>}
 */
function waitForVolumeAttach(ec2, volumeID, instanceID, maxRetries, retryInterval) {
	return new Promise(function(resolve, reject) {
		const
			attempt = () => {
				ec2.describeVolumes({
					VolumeIds: [
						volumeID
					]
				}).promise()
					.then(data => {
						for (let volume of data.Volumes) {
							if (volume.VolumeId === volumeID && volume.Attachments.find(attachment => attachment.State === "attached" && attachment.InstanceId === instanceID)) {
								return resolve(volume);
							}
						}
						
						if (maxRetries <= 0) {
							reject("Timed out waiting for " + volumeID + " to attach to " + instanceID + " (is it stuck in the 'attaching' state? It may need to be force detached)");
						} else {
							maxRetries--;
							setTimeout(attempt, retryInterval || 4000);
						}
					})
					.catch(reject);
			};
		
		attempt();
	});
}


module.exports.pickAvailableAttachmentPoint = pickAvailableAttachmentPoint;

module.exports.waitForVolumeState = waitForVolumeState;
module.exports.waitForVolumeAttach = waitForVolumeAttach;
module.exports.waitForVolumePartitions = waitForVolumePartitions;

module.exports.identifyPartitionsForAttachedVolume = identifyPartitionsForAttachedVolume;
