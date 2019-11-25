/**
 * Module dependencies
 */

var path = require("path");
var Writable = require("stream").Writable;
var Transform = require("stream").Transform;
var concat = require("concat-stream");
var _ = require("lodash");
var oss = require("ali-oss");
var mime = require("mime");
var co = require("co");

/**
 * skipper-aliyunoss
 *
 * @param  {Object} globalOpts
 * @return {Object}
 */

module.exports = function SkipperAliyunOSS(globalOpts) {
  globalOpts = globalOpts || {};

  return {
    read: function aliyunRead(fd, cb) {
      var prefix = fd;
      var store = oss({
        accessKeyId: globalOpts.key,
        accessKeySecret: globalOpts.secret,
        bucket: globalOpts.bucket,
        region: globalOpts.region || undefined,
        secure: true
      });

      // Build a noop transform stream that will pump the OSS output through
      var transform = new Transform();
      transform._transform = function(chunk, encoding, callback) {
        return callback(null, chunk);
      };

      co(store.getStream(prefix))
        .then(function(getStreamResult) {
          // check whether we got an actual file stream:
          if (getStreamResult.res.status < 300) {
            getStreamResult.stream.once("error", function(err) {
              transform.emit("error", err);
            });
            getStreamResult.stream.pipe(transform);
          } else {
            var err = new Error();
            err.status = getStreamResult.res.status;
            err.headers = getStreamResult.res.headers;
            err.message =
              "Non-200 status code returned from Aliyun for requested file.";
            transform.emit("error", err);
          }
        })
        .catch(function(err) {
          transform.emit("error", err);
        });

      if (cb) {
        var firedCb;
        transform.once("error", function(err) {
          if (firedCb) return;
          firedCb = true;
          cb(err);
        });
        transform.pipe(
          concat(function(data) {
            if (firedCb) return;
            firedCb = true;
            cb(null, data);
          })
        );
      }

      return transform;
    },

    rm: function aliyunRM(fd, cb) {
      var store = oss({
        accessKeyId: globalOpts.key,
        accessKeySecret: globalOpts.secret,
        bucket: globalOpts.bucket,
        region: globalOpts.region || undefined,
        secure: true
      });

      co(store.delete(fd))
        .then(function(result) {
          if (result.status == 200) {
            cb();
          } else {
            cb({
              statusCode: result.status,
              message: result
            });
          }
        })
        .catch(function(err) {
          cb(err);
        });
    },
    ls: function aliyunLS(dirname, cb) {
      var store = oss({
        accessKeyId: globalOpts.key,
        accessKeySecret: globalOpts.secret,
        bucket: globalOpts.bucket,
        region: globalOpts.region || undefined,
        secure: true
      });

      // Strip leading slash from dirname to form prefix
      var prefix = dirname.replace(/^\//, "");

      co(store.list({ prefix: prefix }))
        .then(function(result) {
          if (result && result.objects) {
            cb(
              null,
              result.objects.map(function(object) {
                return object.name;
              })
            );
          } else {
            cb(null, []);
          }
        })
        .catch(function(err) {
          cb(err, null);
        });
    },

    receive: function aliyunReceiver(options) {
      options = options || {};
      options = _.defaults(options, globalOpts);
      var headers = options.headers || {};

      var receiver = Writable({
        objectMode: true
      });

      receiver._write = function onFile(file, encoding, next) {
        var store = oss({
          accessKeyId: options.key,
          accessKeySecret: options.secret,
          bucket: options.bucket,
          region: globalOpts.region || undefined,
          secure: true
        });

        var mimeType = headers["content-type"] || mime.getType(file.fd);

        co(store.putStream(file.fd, file, { mime: mimeType }))
          .then(function(result) {
            file.url = result.url;
            return co(store.head(file.fd));
          })
          .then(function(result) {
            file.extra = result.res.headers;
            file.byteCount = result.res.headers["content-length"];
            receiver.emit("writefile", file);
            next();
          })
          .catch(function(err) {
            return next({
              incoming: file,
              code: "E_WRITE",
              stack: typeof err === "object" ? err.stack : new Error(err),
              name: typeof err === "object" ? err.name : err,
              message: typeof err === "object" ? err.message : err
            });
          });
      };

      return receiver;
    }
  };
};
