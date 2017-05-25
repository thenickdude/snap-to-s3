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
	commonOptions = [
		{
			name: "help",
			type: Boolean,
			description: "Show this page\n"
		},
		{
			name: "tag",
			type: String,
			defaultValue: "snap-to-s3",
			typeLabel: "[underline]{name}",
			description: "Name of tag you have used to mark snapshots for migration, and to mark created EBS temporary volumes (default: $default)"
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
	
	migrateOptions = [
		{
			name: "migrate",
			type: Boolean,
			defaultValue: false,
			description: "Migrate EBS snapshots to S3"
		},
		{
			name: "all",
			type: Boolean,
			defaultValue: false,
			description: "Migrate all snapshots whose tag is set to \"migrate\""
		},
		{
			name: "one",
			type: Boolean,
			defaultValue: false,
			description: "... or migrate any one snapshot whose tag is set to \"migrate\""
		},
		{
			name: "snapshots",
			type: String,
			multiple: true,
			typeLabel: "[underline]{SnapshotId} ...",
			description: "... or provide an explicit list of snapshots to migrate (tags are ignored)"
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
			name: "sse",
			type: String,
			typeLabel: "[underline]{mode}",
			description: "Enables server-side encryption, valid modes are AES256 and aws:kms"
		},
		{
			name: "sse-kms-key-id",
			type: String,
			requireNotEmpty: true,
			typeLabel: "[underline]{id}",
			description: "KMS key ID to use for aws:kms encryption, if not using the S3 master KMS key"
		}
	],
	
	validateOptions = [
		{
			name: "validate",
			type: Boolean,
			defaultValue: false,
			description: "Validate uploaded snapshots from S3 against the original EBS snapshots (can be combined with --migrate)"
		}
	],
	
	// There are just provided for display to make it obvious that --all --one and --snapshots apply to --validate too
	validateOptionsForDisplayOnly = [
		{
			name: "all",
			type: Boolean,
			defaultValue: false,
			description: "Validate all snapshots whose tag is set to \"migrated\""
		},
		{
			name: "one",
			type: Boolean,
			defaultValue: false,
			description: "... or validate any one snapshot whose tag is set to \"migrated\""
		},
		{
			name: "snapshots",
			type: String,
			multiple: true,
			typeLabel: "[underline]{SnapshotId} ...",
			description: "... or provide an explicit list of snapshots to validate (tags are ignored)"
		}
	],
	
	usageSections = [
		{
			header: "snap-to-s3",
			content: "Creates EBS volumes from snapshots, tars up their files, compresses them with LZ4, and uploads them to S3."
		},
		{
			header: "Migrate snapshots to S3",
			optionList: migrateOptions
		},
		{
			header: "Validate uploaded snapshots",
			optionList: validateOptions.concat(validateOptionsForDisplayOnly)
		},
		{
			header: "Common options",
			optionList: commonOptions
		},
		{
			header: ""
		}
	],

	allOptions = commonOptions.concat(migrateOptions).concat(validateOptions);

let
	options;

Logger.useDefaults();

for (let option of allOptions) {
	if (option.description) {
		option.description = option.description.replace("$default", option.defaultValue);
	}
}

try {
	// Parse command-line options
	options = commandLineArgs(allOptions)
} catch (e) {
	Logger.error("Error: " + e.message);
	options = null;
}

if (options === null || options.help || process.argv.length <= 2) {
	console.log(getUsage(usageSections));
} else {
	Promise.resolve().then(function() {
		for (let option of allOptions) {
			if (option.required && options[option.name] === undefined) {
				throw new OptionsError("Option --" + option.name + " is required!");
			}
			if ((option.required || option.requireNotEmpty) && options[option.name] === null) {
				throw new OptionsError("Option --" + option.name + " requires an argument!");
			}
		}
		
		let
			subjectCount = 0;
		
		if (options.snapshots && options.snapshots.length !== 0) {
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
		
		if (!options.migrate && !options.validate) {
			throw new OptionsError("You must supply at least one of --migrate or --validate");
		}
		
		if (options.sse === "aes256") {
			// Be nice and capitalise things for the user
			options.sse = "AES256";
		} else if (options.sse === null) {
			// If the user specified --sse, but didn't specify an algorithm, default to AES256 like AWS CLI would
			options.sse = "AES256";
		}
		
        let
			snap = new SnapToS3(options);
		
		if (options.migrate) {
			let
				promise;
			
			if (options.all) {
				promise = snap.migrateAllTaggedSnapshots();
			} else if (options.one) {
				promise = snap.migrateOneTaggedSnapshot();
			} else {
				promise = snap.migrateSnapshots(options.snapshots);
			}
			
			return promise.then(
				migrated => {
					if (migrated.length === 0) {
						Logger.error("No snapshots to migrate (snapshots must have tag \"" + options.tag + "\" set to \"migrate\" to be eligible)");
					}
				},
				error => {
					if (error instanceof SnapToS3.SnapshotMigrationError) {
						Logger.get(error.snapshotID).error(error.error);
						Logger.error("");
						Logger.error("Terminating due to fatal errors.");
						process.exitCode = 1;
					} else {
						throw error;
					}
				}
			);
		} else if (options.validate) {
			let
				promise;
			
			if (options.all) {
				promise = snap.validateAllTaggedSnapshots();
			} else if (options.one) {
				promise = snap.validateOneTaggedSnapshot();
			} else {
				promise = snap.validateSnapshots(options.snapshots);
			}
			
			return promise.then(
				successes => {
					if (successes.length === 0) {
						Logger.error("No snapshots to validate (snapshots must have tag \"" + options.tag + "\" set to \"migrated\" to be eligible)");
					} else {
						Logger.info("");
						Logger.info("These snapshots validated successfully:\n" + successes.join("\n"));
					}
				},
				error => {
					if (error instanceof SnapToS3.SnapshotValidationError) {
						Logger.info("");
						
						if (error.successes.length > 0) {
							Logger.info("These snapshots validated successfully:\n" + error.successes.join("\n") + "\n");
						}
						
						Logger.error("These snapshots failed to validate:\n" + Object.keys(error.failures).map(snapshotID => snapshotID + ": " + error.failures[snapshotID]).join("\n\n"));
						process.exitCode = 1;
					} else {
						throw error;
					}
				}
			);
			
		}
	})
	.catch(err => {
		process.exitCode = 1;
		
		if (err instanceof OptionsError) {
			Logger.error(err.message);
		} else if (err instanceof SnapToS3.SnapshotsMissingError) {
			Logger.error(err);
		} else {
			Logger.error("Error: " + err + " " + (err.stack ? err.stack : ""));
			Logger.error("");
			Logger.error("Terminating due to fatal errors.");
		}
	});
}