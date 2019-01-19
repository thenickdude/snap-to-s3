"use strict";

const
	fs = require("fs"),
	zlib = require("zlib"),
	
	CSVParser = require("csv-parse"),
	AWS = require("aws-sdk"),
	moment = require("moment"),
	objectValues = require('object.values'),
	sprintf = require("sprintf-js").sprintf,
	vsprintf = require("sprintf-js").vsprintf,
	
	gunzipMaybe = require("gunzip-maybe");

function SnapCostAnalysis(options) {
	this.setOptions(options);
	
	this.initPromise = Promise.resolve().then(() => {
		this.ec2 = new AWS.EC2();
	});
}

SnapCostAnalysis.prototype.setOptions = function(options) {
	this.options = options;
};

SnapCostAnalysis.prototype.groupSnapshotsByRegionAndVolumeAndFetchDetails = function(snapshots, ownerIDs, regions) {
	let
		promises = regions.map(region => {
			let
				snapshotDescriptions,
				ec2 = new AWS.EC2({region: region});
			
			return ec2.describeSnapshots({
				Filters: [
					{
						Name: "status",
						Values: [
							"completed"
						]
					}
				],
				OwnerIds: ownerIDs
			}).promise()
				.then(data => {
					snapshotDescriptions = data.Snapshots;
				})
				.then(() => ec2.describeVolumes({}).promise())
				.then(data => {
					let
						volumes = data.Volumes,
						snapshotDescByID = {},
						volumesByID = {};
					
					for (let snapshot of snapshotDescriptions) {
						snapshotDescByID[snapshot.SnapshotId] = snapshot;
					}
					
					for (let volume of volumes) {
						volume.snapshots = [];
						volumesByID[volume.VolumeId] = volume;
					}
					
					// Associate all of the snapshots we have billing information for with their respective volumes
					for (let snapshot of snapshots) {
						if (snapshot.region === region) {
							let
								snapshotDescription = snapshotDescByID[snapshot.id];
							
							if (snapshotDescription) {
								let
									volume;
								
								snapshot.description = snapshotDescription;
								
								if (!(snapshotDescription.VolumeId in volumesByID)) {
									// This volume doesn't exist any more, we'll need to make up details for it from the snapshot.
									volume = {
										VolumeId: snapshotDescription.VolumeId,
										Size: snapshotDescription.VolumeSize,
										snapshots: [],
										deleted: true
									};
									
									volumesByID[snapshotDescription.VolumeId] = volume;
									volumes.push(volume);
								} else {
									volume = volumesByID[snapshotDescription.VolumeId];
								}
								
								snapshot.volume = volume;
								volume.snapshots.push(snapshot);
							} else {
								/*
								 * If we have billing information for a snapshot we can't describe, assume it's
								 * deleted and just do nothing with it.
								 */
							}
						}
					}
					
					return volumes.filter(volume => volume.snapshots.length > 0);
				});
		});
	
	return Promise.all(promises).then(regionData => {
		let
			result = {};
		
		regionData.forEach((data, index) => {
			result[regions[index]] = data;
		});
		
		return result;
	});
};

function compareSnapshotDates(a, b) {
	let
		dateA = moment(a.description.StartTime).unix(),
		dateb = moment(b.description.StartTime).unix();
	
	if (dateA < dateb) {
		return -1;
	}
	
	if (dateA > dateb) {
		return 1;
	}
	
	return 0;
}


SnapCostAnalysis.prototype.summarizeDataByRegionAndVolume = function(regions) {
	const
		MONTH_LENGTH = 30 * 86400,
		getMonthlyUsage = snapshot => snapshot.usageAmount * MONTH_LENGTH / snapshot.usageInterval;
	
	for (let regionName in regions) {
		if (regions.hasOwnProperty(regionName)) {
			let
				region = regions[regionName],
				regionSnapshotCost = 0,
				regionSnapshotCount = 0;
			
			for (let volume of region) {
				// In GB-months
				volume.monthlySnapshotUsage = 0;
				volume.monthlySnapshotCost = 0;
				
				for (let snapshot of volume.snapshots) {
					let
						thisSnapshotUsage = getMonthlyUsage(snapshot);
					
					volume.monthlySnapshotUsage += thisSnapshotUsage;
					volume.monthlySnapshotCost += thisSnapshotUsage * snapshot.unblendedRate;
					
					regionSnapshotCount++;
				}
				
				regionSnapshotCost += volume.monthlySnapshotCost;
			}
			
			console.log(sprintf("Region %s ($%.2f/month for %d snapshot%s)", regionName, regionSnapshotCost, regionSnapshotCount, regionSnapshotCount === 1 ? "" : "s"));
			
			let
				sortedVolumes = Object.values(region).sort((a, b) => {
					if (a.monthlySnapshotCost > b.monthlySnapshotCost) {
						return -1;
					}
					if (a.monthlySnapshotCost < b.monthlySnapshotCost) {
						return 1;
					}
					return 0;
				});
			
			for (let volume of sortedVolumes) {
				let
					volumeName,
					snapshotsByDate = volume.snapshots.sort(compareSnapshotDates),
					firstSnapshot = snapshotsByDate[0],
					formatString, formatArgs,
					averageSnapChangePercentage;
					
				if (volume.Tags && volume.Tags.find(tag => tag.Key === "Name")) {
					volumeName = volume.Tags.find(tag => tag.Key === "Name").Value;
				} else {
					volumeName = "";
				}
				
				averageSnapChangePercentage = (volume.monthlySnapshotUsage - getMonthlyUsage(firstSnapshot)) / volume.snapshots.length / volume.Size * 100;
				
				formatString = "%s (%dGB%s): %d GB total, $%.3g/month for %d snapshot%s";
				formatArgs = [
					volume.VolumeId,
					volume.Size,
					volume.deleted ? ", deleted" : (volumeName.length > 0 ? ", " + volumeName : ""),
					Math.round(volume.monthlySnapshotUsage),
					volume.monthlySnapshotCost,
					volume.snapshots.length,
					volume.snapshots.length === 1 ? "" : "s"
				];
				
				if (volume.snapshots.length > 1) {
					formatString += ", average snapshot change %.2g%%";
					formatArgs.push(averageSnapChangePercentage);
				}
				
				console.log(vsprintf(formatString, formatArgs));
				
				for (let i = 0; i < snapshotsByDate.length; i++) {
					let
						snapshot = snapshotsByDate[i],
						snapshotUsage = getMonthlyUsage(snapshot),
						formatString = "  %22s  %s  %4.1f GB",
						formatArgs = [
							snapshot.id,
							moment(snapshot.description.StartTime).format("YYYY-MM-DD"),
							snapshotUsage
						];
					
					if (i > 0) {
						formatArgs.push((snapshotUsage / volume.Size) * 100);
						formatString += " (%.2g%%)";
					}
					
					console.log(vsprintf(formatString, formatArgs));
				}
				
				console.log("");
			}
			
			console.log("");
		}
	}
};

SnapCostAnalysis.prototype.analyzeReport = function(filename) {
	return this.initPromise.then(() => new Promise((resolve, reject) => {
		let
			fileStream,
			
			columns = null,
			
			parser = CSVParser({
				delimiter: ","
			}),
		
			snapshots = {},
			ownerIDs = {},
			mostRecentDateSeen = {};
		
		parser.on("readable", () => {
			let
				record;
			
			while ((record = parser.read()) !== null) {
				if (columns === null) {
					// This is the first line in the file, extract column headers
					columns = {};
					record.forEach((column, columnIndex) => {
						columns[column] = columnIndex;
					});
				} else if (record[columns["lineItem/Operation"]] === "CreateSnapshot") {
					let
						resourceID = record[columns["lineItem/ResourceId"]],
						matches = resourceID.match(/^arn:[^:]+:ec2:([^:]+):(\d+):snapshot\/(snap-[a-z0-9]+)$/);
					
					if (matches) {
						const
							region = matches[1],
							ownerID = matches[2],
							snapshotID = matches[3],
							
							usageDates = record[columns["identity/TimeInterval"]].match(/(.+)\/(.+)/),
							usageStartDate = moment.utc(usageDates[1]).unix(),
							usageEndDate = moment.utc(usageDates[2]).unix(),
							usageInterval = usageEndDate - usageStartDate,
							
							usageAmount = parseFloat(record[columns["lineItem/UsageAmount"]]);
						
						if (this.options.regions && this.options.regions.indexOf(region) === -1) {
							continue;
						}
						
						if (!(region in mostRecentDateSeen)) {
							mostRecentDateSeen[region] = usageStartDate;
						} else {
							mostRecentDateSeen[region] = Math.max(mostRecentDateSeen[region], usageStartDate);
						}
						
						if (!(resourceID in snapshots)) {
							snapshots[resourceID] = {
								id: snapshotID,
								region: region,
								usageStartDate: usageStartDate,
								usageInterval: usageInterval,
								usageAmount: usageAmount,
								unblendedRate: parseFloat(record[columns["lineItem/UnblendedRate"]])
							};
							
							ownerIDs[ownerID] = true;
						} else {
							let
								snapshot = snapshots[resourceID];
							
							// This should just be a day for a daily report or an hour for an hourly one
							snapshot.usageInterval = Math.max(snapshot.usageInterval, usageInterval);
							
							/*
							 * A snapshot's cost might vary during the month depending on the deletion of earlier
							 * snapshots. We only want the most recent usage, since this will be the most predictive
							 * of costs going forwards.
							 */
							if (usageStartDate > snapshot.usageStartDate) {
								snapshot.usageStartDate = usageStartDate;
								snapshot.usageAmount = usageAmount;
							}
						}
					}
				}
			}
		});
		
		parser.on("error", reject);
		
		parser.on("finish", () => {
			let
				regions = Object.keys(mostRecentDateSeen);
			
			ownerIDs = Object.keys(ownerIDs);
			
			// Assume that snapshots that didn't appear in the newest time period have been deleted
			snapshots = Object.values(snapshots).filter(snapshot => snapshot.usageStartDate === mostRecentDateSeen[snapshot.region]);
			
			console.error("Fetching snapshot and volume descriptions...");
			
			this.groupSnapshotsByRegionAndVolumeAndFetchDetails(snapshots, ownerIDs, regions)
				.then(regionData => this.summarizeDataByRegionAndVolume(regionData))
				.then(resolve, reject);
		});
		
		if (filename) {
			console.error("Parsing Cost and Usage report...");
			fileStream = fs.createReadStream(filename);
		} else {
			console.error("Parsing Cost and Usage report from stdin...");
			fileStream = process.stdin;
		}

		if (filename && filename.match(/\.gz$/)) {
			fileStream.pipe(zlib.createGunzip()).pipe(parser);
		} else {
			fileStream.pipe(gunzipMaybe()).pipe(parser);
		}
	}));
};

objectValues.shim();

module.exports = SnapCostAnalysis;

