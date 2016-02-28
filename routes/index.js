var express = require('express');
var router = express.Router();
var async = require("async");
var _ = require("underscore");
var sqlite3 = require("sqlite3");
var ip = require("ip");

var dispatch = require("dispatch-client");
var webhookService = require("webhook-service");

var db = new sqlite3.Database("data/asn_db.sqlite3");


//var RESOLVE_INTERVAL = 1000;
var WEBHOOK_REMOTE = "http://localhost:8000/webhook/asn/";

// Register ourselves with the dispatch server to find and share URIs for services
var dispatcher = new dispatch.Client("http://localhost:20000");
dispatcher.register("service-asn", ["ip"]);

// Setup the new webhook service responder
var webhookedService = new webhookService.Service(WEBHOOK_REMOTE);
webhookedService.useRouter(router);
webhookedService.callResolver(resolveData);
webhookedService.start();

function resolveData(queuedItem, next) {
    console.log("Resolving ANSs for [%s]\n\t%s", queuedItem.uuid, queuedItem.ip);
    
    var raw_ip = queuedItem.ip;
    var ip_int = ip.toLong(raw_ip);
    db.all("select * from ipv4 where ipv4.ip_start_int <= ? and ipv4.ip_end_int >= ?", ip_int, ip_int, function (err, ipv4_rows) {
        if (err) {
            webhookedService.saveResolved(queuedItem.uuid, err);
            webhookedService.tickleWebhook(queuedItem.uuid, next);
        }
        else {
            var entity_id = ipv4_rows[0].id;
            db.all("select * from asn where asn.id = ?", entity_id, function (err, asn_rows) {
                if (err) {
                    webhookedService.saveResolved(queuedItem.uuid, err);
                    webhookedService.tickleWebhook(queuedItem.uuid, next);
                } 
                else {
                    webhookedService.saveResolved(queuedItem.uuid, 
                        {
                            ip: raw_ip,
                            ipv4: ipv4_rows,
                            asn: asn_rows
                        }
                    );    
                    webhookedService.tickleWebhook(queuedItem.uuid, next);
                }
            });
        }
    });
}

/*
    Direct IP based lookup of the ASN dataset
*/
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
