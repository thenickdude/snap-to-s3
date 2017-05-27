"use strict";

const
	assert = require("assert"),
	fs = require("fs"),
	path = require("path"),
	child_process = require("child_process"),
	
	rmdir = require("rmdir"),
	
	fsTools = require("../lib/filesystem-tools"),
	hashFiles = require("../lib/hash-files");

const
	TEST_STRING_HELLO_WORLD = "Hello, world!",
	TEST_STRING_HELLO_WORLD_MD5 = "6cd3556deb0da54bca060b4c39479839",
	
	TEST_STRING_GOODBYE_WORLD = "Goodbye, world!",
	TEST_STRING_GOODBYE_WORLD_MD5 = "f6da94e9c2d5ac46e9e8ecee3a1731ff",
	
	/**
	 * @typedef {Object} Test
	 * @property {string} name
	 * @property {function} createFiles
	 * @property {string} tarHash
	 * @property {string} tarFilename - Tar filename
	 * @property {string} tarHashesFilename - Filename of list of expected hashes for each file in the tar
	 * @property {int} numNormalFiles - The number of normal (MD5 hashable) files this test generates
	 */
	
	scratchDir = path.resolve(__dirname, ".temp"),
	scratchDir2 = path.resolve(__dirname, ".temp2"),
	tarDir = path.resolve(__dirname, "test-tars"),
	
	/**
	 * @type {Test[]}
	 */
	backupTests = [
		{
			name: "links",
			createFiles: createLinkTest,
			numNormalFiles: 2
		},
		{
			name: "big-random",
			createFiles: createBigRandomFilesTest,
			numNormalFiles: 3
		},
		{
			name: "nested-directories",
			createFiles: createNestedDirectoriesTest,
			numNormalFiles: 3
		},
		{
			name: "special-characters",
			createFiles: createSpecialCharactersTest,
			numNormalFiles: 2
		}
	];

/**
 * snap-to-s3 will compute md5s using Node code, so let's test that against a third-party's
 * file reading + MD5 implementation:
 */
function openSSLMD5File(filename) {
	return new Promise((resolve, reject) => {
		child_process.execFile("openssl", ["md5", filename], (error, stdout, stderr) => {
			if (error) {
				reject("Failed to run openssl " + args.join(" ") + ": " + error + " " + stderr);
			} else {
				let
					tokens = stdout.split("\n")[0].split(" "),
					md5Hash = tokens[tokens.length - 1];
				
				assert(md5Hash.length === 32);
				
				resolve(md5Hash);
			}
		});
	});
}

/**
 * Test files with non-UTF8 character encoding. This only makes sense on some operating systems (Linux). Other
 * operating systems like Windows or Mac would convert the filename to UTF-8 at the time it's stored on disk,
 * mangling it. Linux will preserve the original encoding for us.
 *
 * @param {string} scratchDir
 * @returns {Promise}
 */
function createSpecialCharactersTest(scratchDir) {
	return new Promise((resolve, reject) => {
		if (!scratchDir.match(/\/$/)) {
			scratchDir = scratchDir + "/";
		}
		
		const
			testFilenameUTF8 = "test-En français, s'il vous plaît?.txt",
			
			/* Generate me with:
			 * echo -n "test-En français, s'il vous plaît?.txt" \
			 *     | iconv --from-code=UTF-8 --to-code=LATIN1 \
			 *     | xxd -ps:
			 */
			testFilenameLatin1 = Buffer.from('746573742d456e206672616ee76169732c207327696c20766f757320706c61ee743f2e747874', 'hex');
		
		let
			filename;
		
		// First, a simple UTF-8 encoded filename:
		filename = Buffer.from(scratchDir + testFilenameUTF8, "utf8");
		fs.writeFileSync(filename, TEST_STRING_HELLO_WORLD);
		
		// Now a trickier latin-1 encoded one:
		filename = Buffer.concat([Buffer.from(scratchDir), testFilenameLatin1]);
		fs.writeFileSync(filename, TEST_STRING_GOODBYE_WORLD);
		
		resolve({
			expectedHashes: [
				{filename: testFilenameUTF8, hash: TEST_STRING_HELLO_WORLD_MD5},
				{filename: testFilenameLatin1, hash: TEST_STRING_GOODBYE_WORLD_MD5}
			]
		});
	});
}

function createNestedDirectoriesTest(scratchDir) {
	return new Promise((resolve, reject) => {
		fs.writeFileSync(path.resolve(scratchDir, "test-a.txt"), TEST_STRING_HELLO_WORLD);
		fs.mkdirSync(path.resolve(scratchDir, "test-b"));
		fs.writeFileSync(path.resolve(scratchDir, "test-b", "test-c.txt"), TEST_STRING_GOODBYE_WORLD);
		fs.mkdirSync(path.resolve(scratchDir, "test-b", "test-d"));
		fs.writeFileSync(path.resolve(scratchDir, "test-b", "test-d", "test-c.txt"), "Same filename but different content!");
		
		resolve({
			expectedHashes: [
				{filename: "test-a.txt", hash: TEST_STRING_HELLO_WORLD_MD5},
				{filename: "test-b/test-c.txt", hash: TEST_STRING_GOODBYE_WORLD_MD5},
				{filename: "test-b/test-d/test-c.txt", hash: "fd0a1f8a82e646dfa6cd2691e5bdf495"}
			]
		});
	});
}

function createLinkTest(scratchDir) {
	return new Promise((resolve, reject) => {
		let
			filenameA = path.resolve(scratchDir, "test-a.txt"),
			filenameHardLink = path.resolve(scratchDir, "test-hardlink.txt"),
			filenameSymLink = path.resolve(scratchDir, "test-symlink.txt");
		
		fs.writeFileSync(path.resolve(scratchDir, "test-a.txt"), "Hello, world!");
		fs.linkSync(filenameA, filenameHardLink);
		fs.symlinkSync("test-a.txt", filenameSymLink);
		
		resolve({
			expectedHashes: [
				{filename: "test-a.txt", hash: TEST_STRING_HELLO_WORLD_MD5},
				{filename: "test-hardlink.txt", hash: TEST_STRING_HELLO_WORLD_MD5}
			]
		});
	});
}

function createBigRandomFilesTest(scratchDir) {
	const
		fileSizeMB = 200,
		numFiles = 3;
	
	let
		promise = Promise.resolve(),
		fileHashes = [];
	
	for (let i = 0; i < numFiles; i++) {
		let
			filename = "test-random-" + i,
			outputPath = path.resolve(scratchDir, filename),
			args = ["if=/dev/urandom", "of=" + outputPath, "bs=1024k", "count=" + fileSizeMB];
		
		promise = promise
			.then(() => new Promise((resolve, reject) => {
				console.log("Generating " + filename + " using dd from /dev/urandom...");
				child_process.execFile("dd", args, (error, stdout, stderr) => {
					if (error) {
						reject("Failed to run dd " + args.join(" ") + ": " + error + " " + stderr);
					} else {
						resolve();
					}
				});
			}))
			.then(() => {
				console.log("Hashing " + filename + " with MD5...");
				
				return openSSLMD5File(outputPath)
					.then(hash => fileHashes.push({filename: filename, hash: hash}));
			});
	}
	
	return promise.then(() => ({
		expectedHashes: fileHashes
	}));
}

function emptyScratchDir() {
	return new Promise((resolve, reject) => {
		fs.readdir(scratchDir, {encoding: "buffer"}, (error, files) => {
			if (error) {
				reject(error);
			} else {
				resolve(files);
			}
		});
	}).then(files => {
		for (let file of files) {
			if (!file.toString().match(/^test-/)) {
				throw "There's a file '" + file + "' in the scratch directory " + scratchDir + " that doesn't start with 'test-'! Am I emptying the right directory?";
			}
		}
		
		let
			promise = Promise.resolve(),
			filePrefix = Buffer.from(scratchDir.match(/\/$/) ? scratchDir : scratchDir + "/");
		
		for (let file of files) {
			let
				filename = Buffer.concat([filePrefix, file]);
			
			promise = promise.then(() => new Promise((resolve, reject) => {
				rmdir(filename, (err) => {
					if (err) {
						reject(err);
					} else {
						resolve();
					}
				});
			}));
		}
		
		return promise;
	});
}

function createTarFromDirectory(tarFilename, directory) {
	return new Promise((resolve, reject) => {
		let
			args = ["-cf", tarFilename, "."];
		
		child_process.execFile("tar", args, {
			cwd: directory
		}, (error, stdout, stderr) => {
			if (error) {
				reject("tar " + args.join(" ") + " failed! " + stderr);
			} else {
				resolve();
			}
		});
	});
}

function extractTarToDirectory(tarFilename, directory) {
	return new Promise((resolve, reject) => {
		let
			args = ["-xf", tarFilename];
		
		child_process.execFile("tar", args, {
			cwd: directory
		}, (error, stdout, stderr) => {
			if (error) {
				reject("tar " + args.join(" ") + " failed! " + stderr);
			} else {
				resolve();
			}
		});
	});
}

/**
 * Create tars for all the backupTests (if they don't already exist) and add a "tarFilename" field to each test
 * which points to the generated .tar files.
 *
 * @returns {Promise}
 */
function createTestTars() {
	return fsTools.forcePath(scratchDir)
		.then(() => fsTools.forcePath(tarDir))
		.then(() => {
			let
				promise = Promise.resolve();
			
			for (let test of backupTests) {
				test.tarFilename = path.resolve(tarDir, test.name + ".tar");
				test.tarHashesFilename = test.tarFilename + ".expected.md5s";
				
				if (!fs.existsSync(test.tarFilename) || !fs.existsSync(test.tarHashesFilename)) {
					promise = promise
						.then(() => {
							console.log("Generating " + test.tarFilename);
						})
						.then(emptyScratchDir)
						.then(() => test.createFiles(scratchDir))
						.then(
							createFilesResult => createTarFromDirectory(test.tarFilename, scratchDir)
								.then(() => new Promise((resolve, reject) => {
									const
										outputFile = fs.createWriteStream(test.tarHashesFilename, {defaultEncoding: null}),
										serializer = new hashFiles.FileHashesSerializer();
									
									console.log("Generating " + test.tarHashesFilename);
									
									for (let stream of [outputFile, serializer]) {
										stream.on("error", err => {
											console.log(err);
											reject(err);
										});
									}
								
									outputFile.on("close", () => {
										resolve();
									});
									
									for (let hash of createFilesResult.expectedHashes) {
										serializer.write(hash);
									}
									
									serializer.end();
									
									serializer.pipe(outputFile);
								}))
						)
						.then(() => {
							console.log("");
						});
				}
			}
			
			return promise;
		});
}

module.exports = {
	backupTests: backupTests,
	scratchDir: scratchDir,
	scratchDir2: scratchDir2,
	
	extractTarToDirectory: extractTarToDirectory,
	
	emptyScratchDir: emptyScratchDir,
	createTestTars: createTestTars,
	openSSLMD5File: openSSLMD5File
};