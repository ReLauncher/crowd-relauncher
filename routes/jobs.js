var express = require('express');
var requestify = require('requestify');
var router = express.Router();
var _ = require('lodash');

var bodyParser = require('body-parser');

var CrowdFlower = {
    base: 'https://api.crowdflower.com/',
    version: 'v1/',
    getEndpoint: function(api_key, resource) {

        return CrowdFlower.base + CrowdFlower.version + resource + '.json' + '?key=' + api_key
    }
}

var resetRoute = router.route('/reset');
resetRoute.get(function(request, response) {
    response.redirect('/');
});

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
// Order units in a given job
// =====================================================
router.route('/:id/order')
    .post(parseUrlencoded, function(request, response) {
        var job_id = request.params.id;
        var api_key = request.body.api_key;
        // TODO: in production - use real channels + units_count = all job units
        var post_data = {
            "channels": ["cf_internal"],
            "debit": {
                "units_count": 1
            }
        }
        requestify.post(CrowdFlower.getEndpoint(api_key, 'jobs/' + job_id + '/orders'), post_data)
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
        // TODO: in production - use real channels + units_coun
        // t = all job units

        var Launcher = {
            'api_key': request.body.api_key,
            'job_id': request.params.id,
            'interval': 1 * 60 * 1000,
            'duration_limit': 5 * 60 * 1000,
            'iteration': 0
        }


        console.log('get info');
        getJobInfo(Launcher, function() {
            //console.log('launch the job');
            //launchJob(Launcher, function(Launcher) {
                console.log('start timer');
                Launcher['timer'] = setInterval(function() {
                    periodicCheck(Launcher);
                }, Launcher.interval);
            //});
        });

        function periodicCheck(Launcher) {
            Launcher.iteration++;
            console.log('iteration ' + Launcher.iteration)
            getJobInfo(Launcher, function() {
                if (Launcher.job_info.state != 'running') {
                    console.log('Timer is stopped, because the job state is ' + Launcher.job_info.state);
                    clearInterval(Launcher.timer);
                }
                console.log('job info collected')
                getUnits(Launcher, function() {
                    console.log('units list collected');
                    console.log(Launcher.units);

                    for (var unit_id in Launcher.units) {
                        console.log('unit details for ' + unit_id);
                        getUnitDetail(Launcher, unit_id, function(unit_info) {
                            console.log(unit_info);
                            console.log(unit_info.state);
                            console.log(unit_info.results.judgments.length);
                            console.log(unit_info.data.crowdlauncher);
                            // check how long the unit is in judging mode. Re-assign if necessary
                            if (unit_info.state == "judging") {
                                console.log(unit_info);
                                if (needReassignment(unit_info, Launcher.duration_limit)) {

                                    newUnit(Launcher, unit_info.data, function(new_unit_info) {
                                        console.log('update new unit - to judgable');

                                        updateUnit(Launcher, new_unit_info.id, {
                                            "state": "judgable"
                                        });
                                        // Link new Unit to old Unit
                                        var new_data = new_unit_info.data;
                                        new_data['crowdlauncher'] = {
                                            "parent_id": unit_info.id,
                                            "status": "NF"
                                        }
                                        updateUnit(Launcher, new_unit_info.id, {
                                            "data": new_data
                                        });
                                        // Link old Unit to new Unit
                                        var old_data = unit_info.data;
                                        if (old_data['crowdlauncher']) {
                                            if (old_data['crowdlauncher']['parent_id']) {
                                                old_data['crowdlauncher']['child_id'] = new_unit_info.id;
                                            }
                                        } else {
                                            old_data['crowdlauncher'] = {
                                                "child_id": new_unit_info.id,
                                                "status": "NF"
                                            }
                                        }

                                        updateUnit(Launcher, unit_info.id, {
                                            "data": old_data
                                        });

                                        console.log('cancel the unit ' + unit_info.id);
                                        cancelUnit(Launcher, unit_info.id);
                                    });
                                }

                            }

                            // check if the unit is cancelled, but we received the judgments 
                            if (unit_info.state == "canceled" &&
                                unit_info.results.judgments.length > 0 &&
                                unit_info.data.crowdlauncher.status == "NF") {
                                console.log('we finilizing the cancelled task');

                                updateUnit(Launcher, unit_info.id, {
                                    "state": "finalized"
                                });
                            }

                            if (unit_info.state == "finalized" && unit_info.results.judgments.length > 0 &&
                                unit_info.data.crowdlauncher ) {
                                //&& unit_info.data.crowdlauncher.status == "NF"
                                console.log('we are in a finalized task');

                                var data = unit_info.data;
                                data['crowdlauncher']['status'] = "FN";
                                updateUnit(Launcher, unit_info.id, {
                                    "data": data
                                });
                                // Go deep to kids
                                function meetNodes(node_id, field_name) {
                                    getUnitDetail(Launcher, node_id, function(node_unit_info) {
                                        console.log('checking '+field_name+ ' '+ node_id);
                                        if (node_unit_info.data.crowdlauncher.status == "NF" && node_unit_info.state == 'canceled') {
                                            var node_data = node_unit_info.data;
                                            node_data['crowdlauncher']['status'] = "FN";
                                            updateUnit(Launcher, node_unit_info.id, {
                                                "data": node_data
                                            });
                                            if (['judging', 'judgable'].indexOf(node_unit_info.state) >= 0) {
                                                cancelUnit(Launcher, node_unit_info.id);
                                            }
                                            if (node_unit_info.data.crowdlauncher[field_name]) {
                                                next_node_id = node_unit_info.data.crowdlauncher[field_name];
                                                meetParents(next_node_id, field_name);
                                            }
                                        }
                                    });
                                }
                                if (unit_info.data.crowdlauncher.child_id) {
                                    console.log('go through kids');
                                    var child_id = unit_info.data.crowdlauncher.child_id;
                                    meetNodes(child_id, 'child_id');
                                }
                                // Go deep to parents
                                if (unit_info.data.crowdlauncher.parent_id) {
                                    console.log('go through parents');
                                    var parent_id = unit_info.data.crowdlauncher.parent_id;
                                    meetNodes(parent_id, 'parent_id');
                                }
                            }
                        });
                    }
                    console.log(Launcher.units);
                });
            });
        }
        response.json('the rocket is launched');

    });


function getJobInfo(launcher, callback) {
    requestify.get(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id))
        .then(function(crowdflower_resp) {
            launcher['job_info'] = crowdflower_resp.getBody();
            callback()
        });
}

function getUnits(launcher, callback) {
    requestify.get(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/units'))
        .then(function(crowdflower_resp) {
            launcher['units'] = crowdflower_resp.getBody();
            callback()
        });
}

function getUnitDetail(launcher, unit_id, callback) {
    requestify.get(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/units/' + unit_id))
        .then(function(crowdflower_resp) {
            var unit_info = crowdflower_resp.getBody()
            callback(unit_info);
        });
}



function launchJob(launcher, callback) {
    var post_data = {
        "channels": ["on_demand", "cf_internal"],
        "debit": {
            "units_count": launcher.job_info.units_count
        }
    }
    requestify.post(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/orders'), post_data)
        .then(function(crowdflower_resp) {
            console.log(crowdflower_resp.getBody());
            callback();
        });
}

function cancelUnit(launcher, unit_id) {

    requestify.post(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/units/' + unit_id + '/cancel'))
        .then(function(crowdflower_resp) {
            console.log(crowdflower_resp.getBody());
        });
}

function updateUnit(launcher, unit_id, data) {
    var put_data = {
        "unit": data
    }
    requestify.put(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/units/' + unit_id), put_data)
        .then(function(crowdflower_resp) {
            console.log(crowdflower_resp.getBody());
        });
}

function newUnit(launcher, data, callback) {
    var post_data = {
        "unit": {
            "data": data
        }
    }
    requestify.post(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/units'), post_data)
        .then(function(crowdflower_resp) {
            var unit_info = crowdflower_resp.getBody();
            callback(unit_info)
        });
}

function needReassignment(unit_info, duration_limit) {
    if (unit_info.state == "judging") {
        var delay = calculateDelay(unit_info);
        console.log('####### delay is ' + delay);
        if (delay > duration_limit)
            return true;
    }
    return false;
}

function calculateDelay(unit_info) {
    var start = new Date(unit_info.updated_at);
    var end = new Date();
    return end - start;
}

module.exports = router;
