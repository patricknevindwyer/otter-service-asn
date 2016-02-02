var express = require('express');
var router = express.Router();
var request = require('request');
var dns = require('dns');
var async = require("async");
var _ = require("underscore");
var sqlite3 = require("sqlite3");
var ip = require("ip");

var db = new sqlite3.Database("data/asn_db.sqlite3");

var RESOLVE_INTERVAL = 1000;
var WEBHOOK_REMOTE = "http://localhost:8000/webhook/asn/";

/*
    This module conforms to a standard Webhook based round trip.
    
    1. POST /resolve  (JSON body) - data to resolve
    2. enqueue data for resolve
    3. interval check for resolve data in queue
    4. Resolve top item from queue
    5. Store resolved data
    6. Tickle remote webhook for "completed" state
      6a. GET <remote>:/<webhook>/<item_id>/resolved
    7. GET /resolved/<item_id>
    8. DELETE /resolved/<item_id>

*/

var RESOLVE_QUEUE = [];
var RESOLVED_DATA = {};

function resolveData(queuedItem, next) {
    console.log("Resolving ANSs for [%s]\n\t%s", queuedItem.uuid, queuedItem.ip);
    
    var raw_ip = queuedItem.ip;
    var ip_int = ip.toLong(raw_ip);
    db.all("select * from ipv4 where ipv4.ip_start_int <= ? and ipv4.ip_end_int >= ?", ip_int, ip_int, function (err, ipv4_rows) {
        if (err) {
            RESOLVED_DATA[queuedItem.uuid] = err;
            tickleWebhook(queuedItem.uuid + "/ready", next);
        }
        else {
            var entity_id = ipv4_rows[0].id;
            db.all("select * from asn where asn.id = ?", entity_id, function (err, asn_rows) {
                if (err) {
                    RESOLVED_DATA[queuedItem.uuid] = err;
                    tickleWebhook(queuedItem.uuid + "/ready", next);
                } 
                else {
                                
                    RESOLVED_DATA[queuedItem.uuid] = {
                            ip: raw_ip,
                            ipv4: ipv4_rows,
                            asn: asn_rows
                        };    
                    tickleWebhook(queuedItem.uuid + "/ready", next);
                }
            });
        }
    });
}

function tickleWebhook(path, next) {
    request(WEBHOOK_REMOTE + path, function (error, response, body) {
        if (!error && response.statusCode == 200) {
            next();
        }
        else {
            console.log("Error calling remote webook at [%s]\n\tcode: %d\n\terror: %s", WEBHOOK_REMOTE + path, response.statusCode, error);
            next();
        }
    })   
}

/*
    Generic queue check and drain that kicks off at most
    every RESOLVE_INTERVAL milliseconds. 
*/
function checkResolveQueue() {
    
    if (RESOLVE_QUEUE.length > 0) {
        var resolveItem = RESOLVE_QUEUE.shift();
        resolveData(resolveItem, 
            function () {
                setTimeout(checkResolveQueue, RESOLVE_INTERVAL);
            }
        );
    }
    else {
        setTimeout(checkResolveQueue, RESOLVE_INTERVAL);
    }
}
checkResolveQueue();

/*
    We expect the post body to have an IP address in a JSON structure,
    like:
        {
            "ip": "17.128.100.10",
            "uuid": <uuid>
        }

*/
router.post("/resolve", function (req, res, next) {
    
    RESOLVE_QUEUE.push(req.body);
    res.json({error: false, msg: "ok"});
    
});

router.get(/^\/resolved\/([a-zA-Z0-9\-]+)\/?$/, function (req, res, next) {
    var resolveUuid = req.params[0];
    console.log("Results being retrieved for [%s]", resolveUuid);
    if (RESOLVED_DATA[resolveUuid] !== undefined) {
        res.json({error: false, result: RESOLVED_DATA[resolveUuid]});
    }
    else {
        console.log("Invalid UUID specified");
        res.json({error: true, msg: "No such resolved UUID"});
    }
});

router.delete(/^\/resolved\/([a-zA-Z0-9\-]+)\/?$/, function (req, res, next) {
    var resolveUuid = req.params[0];
    console.log("Deleting results for [%s]", resolveUuid);
    delete RESOLVED_DATA[resolveUuid];
    res.json({error: false, msg: "ok"});
});

router.get(/^\/ip\/(([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+))\/?$/, function (req, res, next) {
    var ip_int = ip.toLong(req.params[0]);
    console.time(req.params[0] + "-query");
    
    db.all("select * from ipv4 where ipv4.ip_start_int <= ? and ipv4.ip_end_int >= ?", ip_int, ip_int, function (err, ipv4_rows) {
        if (err) {
            res.json({error: true, msg: err});
        }
        else {
            var entity_id = ipv4_rows[0].id;
            db.all("select * from asn where asn.id = ?", entity_id, function (err, asn_rows) {
                if (err) {
                    res.send({error: true, msg: err});
                } 
                else {
                    console.timeEnd(req.params[0] + "-query");
                    res.json(
                        {
                            error: false,
                            result: {
                                ip: req.params[0],
                                ipv4: ipv4_rows,
                                asn: asn_rows
                            }
                        }
                    )
                }
            });
        }
    });
});


module.exports = router;
