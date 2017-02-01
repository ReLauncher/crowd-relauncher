var requestify = require('requestify');
var fs = require('fs');
var log = require('npmlog');
var exec = require('child_process').exec;
var Unit = require('./unit');
var CrowdFlower = require('./crowdflower');

var moduletitle = 'relauncher';

module.exports = function(api_key, job_id) {
    this.api_key = api_key;
    this.job_id = job_id;
    this.interval = 1 * 60 * 1000;
    this.duration_limit = 10 * 60 * 1000;
    this.iteration = 0;
    this.toRelaunch = [];
    this.relaunched = [];
};

module.exports.prototype = {
    launchRocket: function(callback) {
        var launcher = this;
        launcher.getJobInfo(function() {
            log.info(moduletitle, 'launch the job');
            //launcher.launchJob(function() {
            //	log.info(moduletitle,'job launched');
                launcher.initTimer();
                callback();
            //});
        });

    },
    initTimer: function() {
        var launcher = this;
        log.info(moduletitle, 'start timer');
        launcher.periodicCheck();
        launcher['timer'] = setInterval(function() {
            launcher.periodicCheck();
        }, launcher.interval);
    },

    periodicCheck: function() {
        var launcher = this;

        launcher.iteration++;
        log.info(moduletitle, 'iteration ' + launcher.iteration);
        launcher.getJobInfo(function() {
            if (launcher.job_info.state != 'running') {
            	log.info(moduletitle,'Timer is stopped, because the job state is ' + launcher.job_info.state);
                clearInterval(launcher.timer);
            }
            log.info(moduletitle, 'job info collected');
            launcher.getUnits(function() {
                log.info(moduletitle, ' units list collected');
                launcher.generateResultsJob(function() {
                    setTimeout(function() {
                        launcher.collectBehaviorLogs(function(){
                            launcher.runRPredictionLoqQualityBehavior(function() {
                                launcher.obtainUnitsToRelaunch(function(toRelaunch){
                                    log.info(moduletitle, typeof(toRelaunch));
                                    launcher.toRelaunch = toRelaunch;
                                    log.info(moduletitle, ' UNITS: '+launcher.units);
                                    var units = [];
                                    for (var unit_id in launcher.units) {
                                        units[unit_id] = new Unit(launcher, unit_id);
                                        units[unit_id].getDetail(function(unit_with_details) {
                                            unit_with_details.process();
                                        });
                                    }
                                });
                            });
                        });
                        //Launcher.runRPredictionClosedTabs(function() {
                            //launcher.obtainLimit(function(limit, completed) {
                                //if (completed / launcher.job_info.units_count >= 0.7) {
                                    //launcher.duration_limit = parseFloat(limit * 1000);
                                    /*var units = [];
                                    for (var unit_id in launcher.units) {
                                        units[unit_id] = new Unit(launcher, unit_id);
                                        units[unit_id].getDetail(function(unit_with_details) {
                                        	unit_with_details.process();
                                        });
                                    //}*/
                                //}
                            //});
                        //});
                    }, 3000);
                });
            });
        });
    },
    runRPredictionLinearRegression: function(callback) {
    	var launcher = this;
        var child = exec('Rscript R/predictLastDuration.R ' + launcher.job_id + ' ' + launcher.api_key + ' ' + launcher.job_info.units_count, function(error, stdout, stderr) {
            log.info(moduletitle, 'R output ' + stdout);
            log.info(moduletitle, 'R errors ' + stderr);
            callback();
        });
    },
    runRPredictionClosedTabs: function(callback) {
        var launcher = this;
        var child = exec('Rscript R/predictAbandonedTabVisibility.R ' + launcher.job_id + ' ' + launcher.api_key, function(error, stdout, stderr) {
            log.info(moduletitle, 'R output ' + stdout);
            log.info(moduletitle, 'R errors ' + stderr);
            callback();
        });
    },
    runRPredictionLoqQualityBehavior: function(callback) {
        var launcher = this;
        var child = exec('Rscript R/predictLowQualityBehavior.R ' + launcher.job_id + ' ' + launcher.api_key, function(error, stdout, stderr) {
            log.info(moduletitle, 'R output ' + stdout);
            log.info(moduletitle, 'R errors ' + stderr);
            callback();
        });
    },
    obtainLimit: function(callback) {
    	var launcher = this;

        var pred = JSON.parse(fs.readFileSync('Datasets/Limits/' + launcher.job_id + '.json', 'utf8'));
    	log.info(moduletitle,'predictions '+pred);
        callback(pred.limit, pred.completed);
    },
    obtainUnitsToRelaunch: function(callback) {
        var launcher = this;
        try {
            var pred = JSON.parse(fs.readFileSync('Datasets/ToRelaunch/' + launcher.job_id + '.json', 'utf8'));
            log.info(moduletitle,'assignments to relaunch '+pred);
            log.info(moduletitle,(typeof pred));
            callback(pred);
        } catch (e) {
            log.info(moduletitle,e);
        }
    },
    collectBehaviorLogs: function(callback){
        var launcher = this;
        var child = exec('./Scripts/generate_logs.sh ' + launcher.job_id, function(error, stdout, stderr) {
            log.info(moduletitle, 'Node ids maker output ' + stdout);
            if (stderr){
                log.info(moduletitle, 'Node ids maker errors ' + stderr);
            }
            callback();
        });
    },
    createUnit: function(data, callback) {
    	log.info(moduletitle,'creating new unit...');
        var launcher = this;
        var post_data = {
            "unit": {
                "data": data
            }
        }
        requestify.post(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/units'), post_data)
            .then(function(crowdflower_resp) {
            	log.info(moduletitle,'new unit created');
                var unit_info = crowdflower_resp.getBody();
                var unit = new Unit(launcher, unit_info.id);
                unit.info = unit_info;
                if (callback)
                    callback(unit);
            });
    },
    meetNodes: function(node_id, field_name) {
        var launcher = this;
        var node = new Unit(launcher, node_id);

        node.getDetail(function(node) {
            log.info(moduletitle, 'checking ' + field_name + ' ' + node.id);
            // && node_unit_info.state == 'canceled'
            if (node.info.data.crowdlauncher.status == "NF") {
                var node_data = node.info.data;
                node_data['crowdlauncher']['status'] = "FN";
                node.update({
                    "data": node_data
                });
                if (['judging', 'judgable'].indexOf(node.info.state) >= 0) {
                    node.cancel();
                }
                if (node.info.data.crowdlauncher[field_name]) {
                    next_node_id = node.info.data.crowdlauncher[field_name];
                    launcher.meetNodes(next_node_id, field_name);
                }
            }
        });
    },
    generateResultsJob: function(callback) {
        var launcher = this;
        log.info(moduletitle, 'generating the results file...');
        requestify.post(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/regenerate', '', '&type=full'))
            .then(function(crowdflower_resp) {
            	log.info(moduletitle, 'the results file is generated');
                callback();
            });
    },
    getJobInfo: function(callback) {
        var launcher = this;
        requestify.get(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id))
            .then(function(crowdflower_resp) {
                launcher['job_info'] = crowdflower_resp.getBody();
                callback(launcher);
            });
    },
    getUnits: function(callback) {
        var launcher = this;
        requestify.get(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/units'))
            .then(function(crowdflower_resp) {
                launcher['units'] = crowdflower_resp.getBody();
                callback(launcher);
            });
    },

    launchJob: function(callback) {
        var launcher = this;
        log.info(moduletitle, 'we try to launch the job');
        var post_data = {
            "channels": ["on_demand", "cf_internal"],
            "debit": {
                "units_count": launcher.job_info.units_count
            }
        }
        requestify.post(CrowdFlower.getEndpoint(launcher.api_key, 'jobs/' + launcher.job_id + '/orders'), post_data)
            .then(function(crowdflower_resp) {
            	log.info(moduletitle, crowdflower_resp.getCode());
                log.info(moduletitle, crowdflower_resp.getBody());
                callback(launcher);
            });
    }
}
