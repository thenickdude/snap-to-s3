const
	child_process = require("child_process"),
	pipe = require("multipipe");

/**
 * @typedef {Promise} ProcessPromise
 * @property {stream.Writable} stdin
 * @property {stream.Readable} stdout
 * @property {stream.Readable} stderr
 */

/**
 * Spawn a process and return its streams, along with a promise that resolves if the process exits normally.
 *
 * @param {string} filename
 * @param {string[]} args
 * @param {Object} options
 *
 * @returns {ProcessPromise}
 */
module.exports.spawnAsPromise = function(filename, args, options) {
	const
		spawned = child_process.spawn(filename, args, options),
	
		result = new Promise((resolve, reject) => {
			for (let stream of [spawned.stdin, spawned.stdout, spawned.stderr]) {
				if (stream) {
					stream.on("error", err => {
						reject(new Error(filename + " stream reported an error: " + err));
					});
				}
			}
			
			spawned.on("error", err => {
				reject(new Error("Executing \"" + filename + " " + args.join(" ") + "\" failed: " + err));
			});
			
			spawned.on("close", code => {
				if (code === 0) {
					resolve();
				} else {
					reject(new Error(filename + " " + args.join(" ") + " returned non-zero exit code: " + code));
				}
			});
		});
	
	result.stdin = spawned.stdin;
	result.stdout = spawned.stdout;
	result.stderr = spawned.stderr;
	
	return result;
};

module.exports.pipelineAsPromise = function() {
	const
		args = Array.prototype.slice.call(arguments);
	
	return new Promise((resolve, reject) => {
		pipe(args, {}, err => {
			if (err) {
				reject(err);
			} else {
				resolve();
			}
		});
	});
};
