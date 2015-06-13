var log = require('npmlog');
var requestify = require('requestify');
var CrowdFlower = require('./crowdflower');

var moduletitle = 'relauncher-unit';


module.exports = function(launcher, id) {
    this.launcher = launcher;
    this.id = id;
};

module.exports.prototype = {
    process: function() {
        var unit = this;
        log.info(moduletitle, 'processing unit [' + unit.id + '] in state ' + unit.info.state);

        if (unit.info.state == "judging") {
            unit.processJudgingUnit();
        }
        if (unit.info.state == "canceled" && unit.info.results.judgments.length > 0 &&
            unit.info.data.crowdlauncher.status == "NF") {
            unit.processCancelledUnit();
        }
        if (unit.info.state == "finalized" && unit.info.results.judgments.length > 0 && unit.info.data.crowdlauncher) {
            unit.processFinalizedUnit();
        }
    },
    processJudgingUnit: function() {
        var unit = this;
        log.info(moduletitle, 'processing judging unit [' + unit.id + ']');

        if (unit.needReassignment()) {
            log.info(moduletitle, 'start relaunching unit [' + unit.id + ']...');
            log.info(moduletitle, unit.info.data);

            var new_data = unit.info.data;
            new_data['crowdlauncher'] = {
                "parent_id": unit.id,
                "status": "NF"
            }
            log.info(moduletitle, new_data);
            unit.launcher.createUnit(new_data, function(new_unit) {

                log.info(moduletitle, 'update new unit - to judgable');
                new_unit.update({
                    "state": "judgable"
                }, function(new_unit) {
                    // Link old Unit to new Unit
                    var old_data = unit.info.data;
                    if (old_data['crowdlauncher']) {
                        if (old_data['crowdlauncher']['parent_id']) {
                            old_data['crowdlauncher']['child_id'] = new_unit.id;
                        }
                    } else {
                        old_data['crowdlauncher'] = {
                            "child_id": new_unit.id,
                            "status": "NF"
                        }
                    }

                    unit.update({
                        "data": old_data
                    }, function(unit) {
                        log.info(moduletitle, 'cancel the unit ' + unit.info.id);
                        unit.cancel();
                    });
                });


            });
        } else {
            log.info(moduletitle, 'not relaunching unit [' + unit.id + ']');
        }
    },
    processCancelledUnit: function() {

        var unit = this;
        log.info(moduletitle, 'processing cancelled unit [' + unit.id + ']');

        unit.update({
            "state": "finalized"
        }, function() {
            unit.info.state = "finalized";
            unit.processFinalizedUnit();
        });
    },
    processFinalizedUnit: function(callback) {
        var unit = this;

        log.info(moduletitle, 'processing finalized unit [' + unit.id + ']');
        //&& unit_info.data.crowdlauncher.status == "NF"
        var data = unit.info.data;
        data['crowdlauncher']['status'] = "FN";
        unit.update({
            "data": data
        }, function() {
            // Go deep to kids
            if (unit.info.data.crowdlauncher.child_id) {
                log.info(moduletitle, 'go through kids');
                var child_id = unit.info.data.crowdlauncher.child_id;
                unit.launcher.meetNodes(child_id, 'child_id');
            }
            // Go deep to parents
            if (unit.info.data.crowdlauncher.parent_id) {
                log.info(moduletitle, 'go through parents');
                var parent_id = unit.info.data.crowdlauncher.parent_id;
                unit.launcher.meetNodes(parent_id, 'parent_id');
            }
        });
    },
    needReassignment: function() {
        var unit = this;
        if (unit.info.state == "judging") {
            var delay = unit.calculateDelay();
            log.info(moduletitle, '####### delay is ' + delay);
            if (delay > unit.launcher.duration_limit)
                return true;
        }
        return false;
    },
    calculateDelay: function() {
        var unit = this;
        var start = new Date(unit.info.updated_at);
        var end = new Date();
        return end - start;
    },
    getDetail: function(callback) {
        var unit = this;
        log.info(moduletitle, 'unit details for ' + unit.id);
        requestify.get(CrowdFlower.getEndpoint(unit.launcher.api_key, 'jobs/' + unit.launcher.job_id + '/units/' + unit.id))
            .then(function(crowdflower_resp) {
                var unit_info = crowdflower_resp.getBody();
                unit.info = unit_info;
                callback(unit);
            });
    },
    cancel: function() {
        var unit = this;
        log.info(moduletitle, 'cancelling unit ' + unit.id + '...');
        requestify.post(CrowdFlower.getEndpoint(unit.launcher.api_key, 'jobs/' + unit.launcher.job_id + '/units/' + unit.id + '/cancel'))
            .then(function(crowdflower_resp) {
                log.info(moduletitle, 'the unit ' + unit.id + ' is cancelled');
            });
    },
    update: function(data, callback) {
        var unit = this;
        var put_data = {
            "unit": data
        }
        log.info(moduletitle, 'updating unit ' + unit.id + '...');
        requestify.put(CrowdFlower.getEndpoint(unit.launcher.api_key, 'jobs/' + unit.launcher.job_id + '/units/' + unit.id), put_data)
            .then(function(crowdflower_resp) {
                var unit_info = crowdflower_resp.getBody();
                unit.info = unit_info;
                log.info(moduletitle, unit.info);
                log.info(moduletitle, 'the unit ' + unit.id + ' is updated');
                if (callback)
                    callback(unit);
            });
    },
}
