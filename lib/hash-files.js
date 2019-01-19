"use strict";

const
	stream = require('stream'),
	fs = require("fs"),
	crypto = require("crypto"),
	assert = require("assert"),

	tar = require("tar-stream"),
	pipe = require("multipipe"),
	
	SimpleProgressStream = require("./simple-progress-stream"),
	BinarySplit = require("binary-split"),
	spawn = require("./spawn");

/**
 * @typedef {Object} FileHash
 * @property {Buffer|String} filename
 * @property {Buffer|String} hash
 *
 * @typedef {FileHash[]} FileHashes
 */

/**
 * Transforms an object stream of filenames into a stream of FileHash objects with their MD5 hashes.
 */
class HashFilesStream extends stream.Transform {
	/**
	 *
	 * @param {String|Buffer} removePrefix - All paths will have this prefix, remove it from them before reporting filenames
	 * @param {function} progress
	 */
	constructor(removePrefix, progress) {
		super({
			objectMode: true
		});
		
		if (typeof removePrefix === "string") {
			this.removePrefix = Buffer.from(removePrefix, "utf8");
		} else if (removePrefix instanceof Buffer) {
			this.removePrefix = removePrefix;
		} else {
			throw "Bad type for prefix to remove";
		}
		
		this.progress = progress || (() => {});
	}

	_transform(filename, enc, cb) {
		if (!(filename instanceof Buffer)) {
			filename = Buffer.from(filename);
		}
		
		assert(filename.compare(this.removePrefix, 0, this.removePrefix.length, 0, this.removePrefix.length) === 0);
		
		const
			fileStream = fs.createReadStream(filename, {
				highWaterMark: 1024 * 1024
			}),
			streamMeter = new SimpleProgressStream(10 * 1024 * 1024),
			hash = md5StreamAsPromise(),
			pipeline = spawn.pipelineAsPromise(fileStream, streamMeter, hash.stdin);

		streamMeter.on("progress", bytes => this.progress(bytes));
		
		Promise.all([hash, pipeline]).then(
			results => {
				cb(null, {filename: filename.slice(this.removePrefix.length), hash: results[0]});
			},
			err => {
				cb(err);
			}
		);
	}
	
	_flush(cb) {
		cb();
	}
}

module.exports.HashFilesStream = HashFilesStream;

function capitalizeFirstLetter(text) {
	if (text.length > 0) {
		text = text.substring(0, 1).toUpperCase() + text.substring(1);
	}
	
	return text;
}

/**
 * @param {string} filename
 * @returns {ProcessPromise}
 */
function spawnSortHashFile(filename) {
	/* We shell out to Unix sort so we can avoid having to keep the whole file in memory. sort performs an
	 * external sort (i.e. using temporary files on disk) of the entries by filename for us.
	 */
	return spawn.spawnAsPromise("sort", ["--key=2", "--zero-terminated", filename], {
		stdio: ["ignore", "pipe", process.stderr],
		env: {
			/* Do a byte-wise sort (don't decode filenames). This sort order needs to match our own in
			 * compareSortedFileHashes(), or else we'll report more differences than really exist.
			 */
			"LC_ALL": "C"
		}
	});
}

/**
 * Return a stream you can pipe into to calculate the MD5 of that stream, and
 * a promise that resolves with the MD5 hash as a hex string.
 *
 * @returns {ProcessPromise} - Promise resolves to the MD5 hash if successful
 */
function md5StreamAsPromise() {
	const
		hashStream = crypto.createHash("md5"),
		
		result = new Promise((resolve, reject) => {
			hashStream.on("readable", () => {
				const
					data = hashStream.read();
				
				if (data) {
					let
						hex = data.toString("hex");
					
					if (hex.length === 32) {
						resolve(hex);
					} else {
						reject("Failed to calculate MD5 hash");
					}
				}
			});
			
			hashStream.on("error", reject);
		});
	
	result.stdin = hashStream;
	result.stdout= null;
	result.stderr = null;
	
	return result;
}

/**
 * Compare two streams which each provide a sorted list of serialized FileHash objects.
 *
 * @param {Readable} hashes1
 * @param {string} hashes1Name
 * @param {Readable} hashes2
 * @param {string} hashes2Name
 *
 * @returns Promise.<int> - Resolves with the number of matched files, or rejects with an array of messages which explain
 * differences
 */
function compareSortedHashListStreams(hashes1, hashes1Name, hashes2, hashes2Name) {
	return new Promise((resolve, reject) => {
		const
			EOF = {};
		
		let
			errors = [],
			files1Count = 0, files2Count = 0,
			
			/**
			 * @type {FileHash|null}
			 */
			file1 = null,
			/**
			 * @type {FileHash|null}
			 */
			file2 = null,
			
			files1Resume = null,
			files2Resume = null,
			
			compareHashes = () => {
				if (file1 !== null && file2 !== null) {
					if (file1 === EOF && file2 === EOF) {
						if (files1Count !== files2Count) {
							errors.push("File counts differ!");
							errors.push(capitalizeFirstLetter(hashes1Name) + ": " + files1Count + " files, " + hashes2Name + ": " + files2Count + " files");
						}
						
						if (errors.length > 0) {
							reject(errors);
						} else {
							resolve(files1Count);
						}
					} else {
						let
							fileOrder;
						
						if (file1 === EOF) {
							fileOrder = 1;
						} else if (file2 === EOF) {
							fileOrder = -1;
						} else {
							fileOrder = file1.filename.compare(file2.filename);
						}
						
						if (fileOrder > 0) {
							errors.push("Only in " + hashes2Name + ": " + file2.filename.toString());
							file2 = null;
							files2Count++;
						} else if (fileOrder < 0) {
							errors.push("Only in " + hashes1Name + ": " + file1.filename.toString());
							file1 = null;
							files1Count++;
						} else {
							// Filenames match
							
							if (file1.hash.compare(file2.hash) !== 0) {
								errors.push("Difference in file: " + file1.filename.toString() + "\n"
									+ capitalizeFirstLetter(hashes1Name) + ": " + file1.hash.toString() + "\n"
									+ capitalizeFirstLetter(hashes2Name) + ": " + file2.hash.toString());
							}
							
							files1Count++;
							files2Count++;
							
							file1 = null;
							file2 = null;
						}
						
						if (file1 === null) {
							files1Resume();
						}
						if (file2 === null) {
							files2Resume();
						}
					}
				}
			},
			
			files1Stream = new stream.Writable({
				objectMode: true,
				write: (file, encoding, callback) => {
					file1 = file;
					files1Resume = callback;
					
					compareHashes();
				}
			}),
			
			files2Stream = new stream.Writable({
				objectMode: true,
				write: (file, encoding, callback) => {
					file2 = file;
					files2Resume = callback;
					
					compareHashes();
				}
			});
		
		files1Stream.on("finish", () => {
			assert(file1 === null);
			file1 = EOF;
			
			compareHashes();
		});
		
		files2Stream.on("finish", () => {
			assert(file2 === null);
			file2 = EOF;
			
			compareHashes();
		});
		
		pipe(hashes1, FileHashesDeserializer(), files1Stream, err => {
			if (err) {
				reject(err);
			}
		});
		
		pipe(hashes2, FileHashesDeserializer(), files2Stream, err => {
			if (err) {
				reject(err);
			}
		});
	});
}

/**
 * Compare two files serialized by FileHashesSerializer and identify any differences.
 *
 * @param {string} hashes1Filename
 * @param {string} hashes1Name
 * @param {string} hashes2Filename
 * @param {string} hashes2Name
 *
 * @returns Promise.<int> - Resolves with the number of matched files, or rejects with an array of messages which explain
 * differences
 */
module.exports.compareHashListFiles = function(hashes1Filename, hashes1Name, hashes2Filename, hashes2Name) {
	// We have to sort the files to compare them:
	const
		sort1 = spawnSortHashFile(hashes1Filename),
		sort2 = spawnSortHashFile(hashes2Filename);
	
	// Both sort processes have to return successfully, and the comparison has to succeed:
	return Promise.all([
		compareSortedHashListStreams(sort1.stdout, hashes1Name, sort2.stdout, hashes2Name),
		sort1,
		sort2,
	]).then(results => results[0]);
};

class FileHashesSerializer extends stream.Transform {
	constructor() {
		super({
			writableObjectMode: true,
			readableObjectMode: false
		});
		
		this.writtenCount = 0;
	}
	
	_transform(fileHash, enc, cb) {
		this.push(fileHash.hash);
		this.push(" ");
		this.push(fileHash.filename);
		this.push("\0");
		
		this.writtenCount++;
		
		cb();
	}
	
	_flush(cb) {
		cb();
	}
}

module.exports.FileHashesSerializer = FileHashesSerializer;

/**
 * Takes a stream of individual file line Buffer objects and produces FileHash objects.
 */
class FileHashDeserializer extends stream.Transform {
	constructor() {
		super({
			writableObjectMode: false,
			readableObjectMode: true
		});
		
		this.offset = 0;
	}

	_transform(line, enc, cb) {
		// Must be at least as long as an MD5 hash plus a space separator (filename may be empty)
		if (line.length >= 32 + 1) {
			cb(null, {
				// We can have both hash and filename share the same original Buffer if we avoid converting them to strings:
				hash: line.slice(0, 32),
				filename: line.slice(33),
				// Byte offset of this entry within the stream:
				offset: this.offset
			});
			
			this.offset += line.length + 1; // +1 for the line terminator
		} else {
			cb(new Error("Line too short (only " + line.length + " long, must be at least 33"));
		}
	}
	
	_flush(cb) {
		cb();
	}
}

function FileHashesDeserializer() {
	return pipe(new BinarySplit("\0"), new FileHashDeserializer());
}

/**
 * Computes an MD5 hash of all regular files in the given directory, and saves that list of file hashes to the given
 * filename. The file can later be read with readHashFileAsStream().
 *
 * @param {String} directory - Directory to be hashed
 * @param {String} filename - Filename to write the hashes to
 * @param {function} progress - called periodically with number of bytes written since last progress
 * @returns {Promise.<int>} Resolves to the number of files hashed
 */
module.exports.hashFilesInDirectory = function(directory, filename, progress) {
	const
		/*
		 * Node only offers fs.readdir() for finding files, which reads an entire directory into memory at once.
		 * That's going to be super expensive for million-file directories (ask me how I know...). Use Unix find
		 * instead so we can stream.
		 */
		find = spawn.spawnAsPromise("find", ["-P", directory, "-type", "f", "-print0"], {
			stdio: ["ignore", "pipe", process.stderr]
		}),
		
		hashFiles = new Promise((resolve, reject) => {
			const
				directoryWithSlash = directory.match(/\/$/) ? directory : directory + "/",
				splitLines = new BinarySplit("\0"),
				hashFiles = new HashFilesStream(directoryWithSlash, progress),
				serializer = new FileHashesSerializer(),
				fileStream = fs.createWriteStream(filename, {
					flags: "w",
					mode: 0o600
				});
			
			pipe(find.stdout, splitLines, hashFiles, serializer, fileStream, err => {
				if (err) {
					reject(err);
				} else {
					resolve(serializer.writtenCount);
				}
			})
		});
	
	return Promise.all([hashFiles, find]).then(results => results[0]);
};

/**
 * Compute an MD5 hash of all regular files in the given tar, and saves that list of file hashes to the given
 * filename. The file can later be read with readHashFileAsStream().
 *
 * @param {stream.Readable} tarStream
 * @param {string} outputFilename
 * @returns {Promise.<int>} - Resolves to the number of regular files hashed.
 */
module.exports.hashTarFilesFromStream = function(tarStream, outputFilename) {
	const
		hardLinkTargets = {},
		hardLinks = {},
		
		fd = fs.openSync(outputFilename, "w+", 0o600),
		
		removeRootPrefix = filename => filename.replace(/^(\.\/|\/)/, "");
	
	let
		haveHardLinks = false;
	
	return new Promise((resolve, reject) => {
		const
			extract = tar.extract({filenameEncoding: "binary"}),
			serializer = new FileHashesSerializer(),
			outputFileStream = fs.createWriteStream("", {
				fd: fd,
				autoClose: false
			});
		
		extract.on('entry', function (fileHeader, fileStream, next) {
			const
				prefixlessFilename = removeRootPrefix(fileHeader.name);
			
			switch (fileHeader.type) {
				case "file":
					const
						md5 = md5StreamAsPromise(),
						pipeline = spawn.pipelineAsPromise(fileStream, md5.stdin);
					
					Promise.all([md5, pipeline]).then(
						results => {
							serializer.write({filename: Buffer.from(prefixlessFilename, "binary"), hash: results[0]});
							
							next();
						},
						err => {
							reject(err);
						}
					);
					break;
				case "link": //Hard link to a file that was already included in the tar stream
					const
						linkTarget = removeRootPrefix(fileHeader.linkname);
					
					// Remember the details of the link in memory so we can resolve it in a second pass
					haveHardLinks = true;
					hardLinkTargets[linkTarget] = true;
					hardLinks[prefixlessFilename] = linkTarget;
					
					// Write a dummy entry that we'll overwrite later
					serializer.write({filename: Buffer.from(prefixlessFilename, "binary"), hash: "00000000000000000000000000000000"});

					fileStream.on('end', function () {
						next();
					});
					
					fileStream.resume(); // Drain the stream
					break;
				default:
					// We don't hash other special file types like symlinks
					
					fileStream.on('end', function () {
						next();
					});
					
					fileStream.resume(); // Drain the stream
			}
		});
		
		tarStream.on("error", reject);
		extract.on("error", reject);
		
		extract.on("finish", () => {
			serializer.end();
		});
		
		outputFileStream.on("finish", () => resolve(serializer.writtenCount));
		
		tarStream.pipe(extract);
		
		serializer.pipe(outputFileStream);
	}).then(writtenCount => {
		/*
		 * Do we have to resolve any hard links? If so, we'll need to patch up the hash file we wrote to resolve
		 * them. This needs to be done before that file gets sorted.
		 *
		 * This is done as a post-process to avoid having to keep the hashes of all files read so far in memory.
		 * This way we only need to remember the hashes of files that were actually hard-linked.
		 */
		if (haveHardLinks) {
			return new Promise((resolve, reject) => {
				const
					inputStream = fs.createReadStream("", {
						fd: fd,
						autoClose: false,
						start: 0
					}),
					deserializer = FileHashesDeserializer();
				
				deserializer.on("readable", () => {
					let
						entry;
					
					while ((entry = deserializer.read()) !== null) {
						let
							entryFilename = entry.filename.toString("binary");
						
						if (entryFilename in hardLinkTargets) {
							// Remember the hash of this file so we can resolve links to it later..
							hardLinkTargets[entryFilename] = entry.hash;
						} else if (entryFilename in hardLinks) {
							// And now we can resolve the link and update the hash at this location
							let
								resolved = hardLinkTargets[hardLinks[entryFilename]];
							
							if (!(resolved instanceof Buffer)) {
								throw "Failed to resolve tar hard link from " + entryFilename + " to " + hardLinks[entryFilename];
							}
							
							fs.writeSync(fd, resolved, 0, resolved.length, entry.offset);
						}
					}
				});
				
				pipe(inputStream, deserializer, (err) => {
					if (err) {
						reject(err);
					} else {
						resolve(writtenCount);
					}
				});
			});
		} else {
			return writtenCount;
		}
	})
	// Ensure the file is closed on both success and failure
	.then(
		writtenCount => {
			fs.closeSync(fd);
			
			return writtenCount;
		},
		err => {
			fs.closeSync(fd);
			
			throw err;
		}
	);
};

module.exports.md5StreamAsPromise = md5StreamAsPromise;