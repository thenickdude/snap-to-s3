module.exports = SimpleProgressStream;

const
	util = require("util"),
	Transform = require("stream").Transform;

function SimpleProgressStream(progressChunkSize) {
	Transform.call(this);
	
	this.bytesSinceLastProgress = 0;
	this.progressChunkSize = progressChunkSize || (64 * 1024);
}

util.inherits(SimpleProgressStream, Transform);

SimpleProgressStream.prototype._transform = function (chunk, encoding, callback) {
	this.bytesSinceLastProgress += chunk.length;
	this.push(chunk);
	
	if (this.bytesSinceLastProgress > this.progressChunkSize) {
		if (this.emit("progress", this.bytesSinceLastProgress)) {
			this.bytesSinceLastProgress = 0;
		}
	}
	
	callback();
};

SimpleProgressStream.prototype._flush = function (callback) {
	// Deliver one last progress event to finish up the stream
	if (this.bytesSinceLastProgress > 0) {
		this.emit("progress", this.bytesSinceLastProgress);
		this.bytesSinceLastProgress = 0;
	}
	
	callback();
};