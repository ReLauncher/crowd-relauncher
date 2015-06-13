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
            response.json('the rocket is launched');
        });

    });

module.exports = router;
