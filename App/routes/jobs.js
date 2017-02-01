var express = require('express');
var requestify = require('requestify');
var router = express.Router();
var _ = require('lodash');
var fs = require('fs');


var bodyParser = require('body-parser');
var resetRoute = router.route('/reset');
resetRoute.get(function(request, response) {
    response.redirect('/');
});


var CrowdFlower = require('../crowdflower');
var ReLauncher = require('../relauncher');
var Unit = require('../unit');

var parseUrlencoded = bodyParser.json();
// =====================================================
// Get all jobs
// =====================================================

router.route('/')
    .post(parseUrlencoded, function(request, response) {
        var api_key = request.body.api_key;
        requestify.get(CrowdFlower.getEndpoint(api_key, 'jobs'))
            .then(function(crowdflower_resp) {
                // Get the response body (JSON parsed or jQuery object for XMLs)
                var jobs = crowdflower_resp.getBody();
                response.json(jobs);
            });
    });
// =====================================================
// Get job info by id
// =====================================================
router.route('/:id')
    .post(parseUrlencoded, function(request, response) {
        var job_id = request.params.id;
        var api_key = request.body.api_key;
        requestify.get(CrowdFlower.getEndpoint(api_key, 'jobs/' + job_id))
            .then(function(crowdflower_resp) {
                var job = crowdflower_resp.getBody();
                response.json(job);
            });
    });

// =====================================================
// Launch a job with re-assignment
// =====================================================
router.route('/:id/launch')
    .post(parseUrlencoded, function(request, response) {
        var launcher = new ReLauncher(request.body.api_key, request.params.id)
        launcher.launchRocket(function() {
            response.json('the application is launched for the job: '+request.params.id);
        });

    });
router.route('/:id/monitor_relaunched_chains')
    .post(parseUrlencoded, function(request, response) {
        var launcher = new ReLauncher(request.body.api_key, request.params.id)
        launcher.initTimer(function() {
            response.json('relaunched_chains are monitored');
        });
    });

router.route('/:job_id/units/:unit_id/relaunch')
    .post(parseUrlencoded, function(request, response) {
        var launcher = new ReLauncher(request.body.api_key, request.params.job_id)
        var the_unit = new Unit(launcher,request.params.unit_id)
        the_unit.getDetail(function(the_unit){
            
            if (the_unit.info.state == "judging"){
                the_unit.relaunchUnit();
                response.json('the unit is relaunching...');
            }else{
                response.json('the unit has status '+the_unit.info.state);
            }
            
        });
    });
module.exports = router;
