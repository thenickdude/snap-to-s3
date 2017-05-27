"use strict";

const
	fs = require("fs"),
	path = require("path"),
	
	fsTools = require("../lib/filesystem-tools"),
	hashFiles = require("../lib/hash-files"),
	
	common = require("./common");

function testFilesystemAndTarHashing() {
	let
		promise = Promise.resolve(),
		
		hashListFilename = path.resolve(common.scratchDir2, "local.md5");
	
	for (let test of common.backupTests) {
		promise = promise
			.then(() => common.emptyScratchDir())
			.then(() => {
				console.log("Extracting '" + test.tarFilename + "' for testing...");
				
				return common.extractTarToDirectory(test.tarFilename, common.scratchDir);
			})
			.then(() => {
				// This checks if we are able to roundtrip our testcases from tar -cf to tar -xf...
				console.log("Comparing unpacked files against expectations from testcase...");
				
				return hashFiles.hashFilesInDirectory(common.scratchDir, hashListFilename, () => {});
			})
			.then(() => hashFiles.compareHashListFiles(
				hashListFilename, "unpacked tar",
				test.tarHashesFilename, "test expectation"
			))
			.then(
				matchedFileCount => {
					if (matchedFileCount !== test.numNormalFiles) {
						throw "Our MD5 signature match found " + matchedFileCount + " files, we expected " + test.numNormalFiles + "!";
					} else {
						console.log("Matched all " + matchedFileCount + " files!");
					}
				},
				errors => {
					if (Array.isArray(errors)) {
						throw errors.join("\n");
					} else {
						throw errors;
					}
				}
			)
			.then(() => {
				/* Can we still roundtrip if we use our own tar reading library to read the tar, instead
				 * of having the official "tar" binary unpack the files to the filesystem for us?
				 */
				console.log("Comparing files from tar using streaming approach against expectations from testcase...");
				return hashFiles.hashTarFilesFromStream(fs.createReadStream(test.tarFilename), hashListFilename);
			})
			.then(() => hashFiles.compareHashListFiles(
				hashListFilename, "tar",
				test.tarHashesFilename, "test expectation"
			))
			.then(
				matchedFileCount => {
					if (matchedFileCount !== test.numNormalFiles) {
						throw "Our MD5 signature match found " + matchedFileCount + " files, we expected " + test.numNormalFiles + "!";
					} else {
						console.log("Matched all " + matchedFileCount + " files!");
					}
				},
				errors => {
					if (Array.isArray(errors)) {
						throw errors.join("\n");
					} else {
						throw errors;
					}
				}
			)
			.then(() => {
				console.log("");
			});
	}
	
	return promise.then(() => {
		try {
			fs.unlinkSync(hashListFilename);
		} catch (e) {
		}
	});
}

common.createTestTars()
	.then(() => fsTools.forcePath(common.scratchDir2))
	.then(() => testFilesystemAndTarHashing())
	.then(
		() => {
			console.log("Done!");
		},
		err => {
			console.error("Fatal error:\n" + err);
			if (err.stack) {
				console.error(err.stack);
			}
			
			process.exitCode = 1;
		}
	);
