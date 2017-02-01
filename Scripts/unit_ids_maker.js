var Firebase = require('firebase');
var requestify = require('requestify');
//var firebase_base_url = 'https://crowdworker-logger.firebaseio.com/' + "taskscrowdflowercom" + '/';
//var Ref = new Firebase(firebase_base_url);
var CROWDFLOWER_API_KEY = process.env.CROWDFLOWER_API_KEY;
var JOB_TO_BE_PROCESSED = process.argv[2];

var Crowdflower = {
    base: "http://api.crowdflower.com/v1/",
    api_key: CROWDFLOWER_API_KEY,
    getUnits: function(job_id, callback) {
        var cf = Crowdflower;
        var crowdflower_units_url = cf.base + "/jobs/" + job_id + "/units.json?key=" + cf.api_key;
        console.log(crowdflower_units_url);
        requestify.get(crowdflower_units_url, {
                headers: {
                    "Accept": "application/json"
                }
            })
            .then(function(response) {
                var units = response.getBody();
                if (callback)
                    callback(units);
            });
    },
    updateUnit: function(job_id, unit_id, data, callback) {
        var cf = Crowdflower;
        var crowdflower_unit_url = cf.base + "/jobs/" + job_id + "/units/" + unit_id + ".json?key=" + cf.api_key;

        var put_data = {
            unit: {
                data: data
            }
        };
        console.log(put_data);
        requestify.put(crowdflower_unit_url, put_data)
            .then(function(crowdflower_resp) {
                var unit_info = crowdflower_resp.getBody();
                if (callback)
                    callback(unit_info);
            });
    }
}

Crowdflower.getUnits(JOB_TO_BE_PROCESSED, function(units) {
    for (var unit_id in units) {
        var unit_data = units[unit_id];
        unit_data['re_unit_id'] = unit_id;
        //console.log(unit_data);
        Crowdflower.updateUnit(JOB_TO_BE_PROCESSED, unit_id, unit_data, function(updated_unit) {
            console.log(updated_unit);
        });
    }
})
