#!/usr/bin/env node

"use strict";

const
	commandLineArgs = require("command-line-args"),
	getUsage = require("command-line-usage"),
	
	Logger = require("js-logger"),
	
	SnapToS3 = require("./lib/snap-to-s3.js");

class OptionsError extends Error {
	constructor(message) {
		super(message);
	}
}

const
	optionDefinitions = [
		{
			name: "help",
			type: Boolean,
			description: "Show this page\n"
		},
		{
			name: "all",
			type: Boolean,
			defaultValue: false,
			description: "Migrate all snapshots whose tag is set to \"migrate\" ..."
		},
		{
			name: "one",
			type: Boolean,
			defaultValue: false,
			description: "... or migrate any one snapshot whose tag is set to \"migrate\" ..."
		},
		{
			name: "snapshots",
			type: String,
			multiple: true,
			typeLabel: "[underline]{SnapshotId} ...",
			description: "... or provide an explicit list of snapshots to migrate (tags are ignored)"
		},
		{
			name: "tag",
			type: String,
			defaultValue: "snap-to-s3",
			typeLabel: "[underline]{name}",
			description: "Name of tag you have used to mark snapshots for migration, and to mark created EBS temporary volumes (default: $default)\n"
		},
		{
			name: "bucket",
			type: String,
			required: true,
			typeLabel: "[underline]{name}",
			description: "S3 bucket to upload to (required)"
		},
		{
			name: "mount-point",
			type: String,
			required: true,
			defaultValue: "/mnt",
			typeLabel: "[underline]{path}",
			description: "Temporary volumes will be mounted here, created if it doesn't already exist (default: $default)"
		},
		{
			name: "upload-streams",
			type: Number,
			defaultValue: 4,
			typeLabel: "[underline]{num}",
			description: "Number of simultaneous streams to send to S3 (increases upload speed and memory usage, default: $default)"
		},
		{
			name: "compression-level",
			type: Number,
			defaultValue: 1,
			typeLabel: "[underline]{level}",
			description: "LZ4 compression level (1-9, default: $default)"
		},
		{
			name: "dd",
			type: Boolean,
			defaultValue: false,
			description: "Use dd to create a raw image of the entire volume, instead of tarring up the files of each partition"
		},
		{
			name: "keep-temp-volumes",
			type: Boolean,
			defaultValue: false,
			description: "Don't delete temporary volumes after we're done with them"
		},
		{
			name: "volume-type",
			type: String,
			defaultValue: "standard",
			typeLabel: "[underline]{type}",
			description: "Volume type to use for temporary EBS volumes (suggest standard or gp2, default: $default)"
		}
	],
	
	usageSections = [
		{
			header: "snap-to-s3",
			content: "Creates EBS volumes from snapshots, tars up their files, compresses them with LZ4, and uploads them to S3."
		},
		{
			header: "Options",
			optionList: optionDefinitions
		}
	];

let
	options;

Logger.useDefaults();

for (let option of optionDefinitions) {
	if (option.description) {
		option.description = option.description.replace("$default", option.defaultValue);
	}
}

try {
	// Parse command-line options
	options = commandLineArgs(optionDefinitions)
} catch (e) {
	Logger.error("Error: " + e.message);
	options = null;
}

if (options === null || options.help || process.argv.length <= 2) {
	console.log(getUsage(usageSections));
} else {
	Promise.resolve().then(function() {
		for (let option of optionDefinitions) {
			if (option.required) {
				if (options[option.name] === undefined) {
					throw new OptionsError("Option --" + option.name + " is required!");
				} else if (options[option.name] === null) {
					throw new OptionsError("Option --" + option.name + " requires an argument!");
				}
			}
		}
		
		let
			subjectCount = 0;
		
		if (options.snapshots.length !== 0) {
			subjectCount++;
		}
		if (options.all) {
			subjectCount++;
		}
		if (options.one) {
			subjectCount++;
		}
		
		if (subjectCount !== 1) {
			throw new OptionsError("You must supply exactly one of --snapshots, --all or --one options");
		}
		
		let
			snap = new SnapToS3(options);
		
		if (options.all || options.one) {
			let
				promise;
			
			if (options.all) {
				promise = snap.migrateAllTaggedSnapshots();
			} else {
				promise = snap.migrateOneTaggedSnapshot();
			}
			
			return promise.then(migratedAny => {
				if (!migratedAny) {
					Logger.error("No snapshots to migrate (snapshots must have tag \"" + options["tag"] + "\" set to \"migrate\" to be eligible)");
				}
			});
		} else {
			return snap.migrateSnapshots(options.snapshots);
		}
	})
	.catch(err => {
		process.exitCode = 1;
		
		if (err instanceof OptionsError) {
			console.error(err.message);
		} else {
			if (err instanceof SnapToS3.SnapshotMigrationError) {
				Logger.get(err.snapshotID).error(err.error);
			} else {
				Logger.error("Error: " + err + " " + (err.stack ? err.stack : ""));
			}
			Logger.error("");
			Logger.error("Terminating due to fatal errors.");
		}
	});
}