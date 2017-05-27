"use strict";

const
	util = require("util"),
	Transform = require("stream").Transform;

class SimpleProgressStream extends Transform {
	constructor(progressChunkSize) {
		super();
		
		this.bytesSinceLastProgress = 0;
		this.progressChunkSize = progressChunkSize || (64 * 1024);
	}
	
	_transform(chunk, encoding, callback) {
		this.bytesSinceLastProgress += chunk.length;
		this.push(chunk);
		
		if (this.bytesSinceLastProgress > this.progressChunkSize) {
			if (this.emit("progress", this.bytesSinceLastProgress)) {
				this.bytesSinceLastProgress = 0;
			}
		}
		
		callback();
	}
	
	_flush(callback) {
		// Deliver one last progress event to finish up the stream
		if (this.bytesSinceLastProgress > 0) {
			this.emit("progress", this.bytesSinceLastProgress);
			this.bytesSinceLastProgress = 0;
		}
		
		callback();
	}
}

module.exports = SimpleProgressStream;
