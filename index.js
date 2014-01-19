/*jslint node: true */
"use strict";

var amqpSchedule = require('amqp-schedule'),
    util = require('util'),
    defaultMinBackoffSeconds = 0.1,
    dfaultMaxDoublings = 19;

module.exports = function wrapper(parameters, cb) {
    return function (message, header, deliveryInfo, job) {
        header.retries = header.retries || 0;

        job.retry = function () {
            var taskRetryLimit = parameters.taskRetryLimit,
                minBackoffSeconds = parameters.minBackoffSeconds || defaultMinBackoffSeconds,
                maxBackoffSeconds = parameters.maxBackoffSeconds,
                maxDoublings = parameters.maxDoublings;

            var delay = Math.pow(2, Math.min(maxDoublings || header.retries, header.retries)) * minBackoffSeconds;

            if (minBackoffSeconds && minBackoffSeconds > delay) {
                delay = minBackoffSeconds;
            }

            if (maxBackoffSeconds && maxBackoffSeconds < delay) {
                delay = maxBackoffSeconds;
            }

            if (taskRetryLimit && header.retries >= taskRetryLimit) {
                // TODO some sort of logging
                return message.reject(false);
            }

            header.retries++;

            var messageOptions = Object.keys(deliveryInfo).reduce(function (obj, key) {
                obj[key] = deliveryInfo[key];
                return obj;
            }, {});

            messageOptions.headers = header;
            amqpSchedule(job.queue.connection)(deliveryInfo.exchange, deliveryInfo.routingKey, message, delay * 1000, messageOptions);
        };

        cb(message, header, deliveryInfo, job);
    };
};
