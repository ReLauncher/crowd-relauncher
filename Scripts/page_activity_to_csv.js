var requestify = require('requestify');
var fs = require('fs');


var firebase_base_url = 'https://crowdworker-logger.firebaseio.com/' + "taskscrowdflowercom" + '/';
var task_id = process.argv[2];
var firebase_target_url = firebase_base_url + task_id + ".json";
var folder = "Datasets/Logs/"+task_id+"/";
var filename = folder + task_id + "_page.csv";

fs.createWriteStream(filename);
fs.truncate(filename, 0, function() {
    console.log('file ' + filename + ' was cleaned up.')
});
fs.appendFile(filename, 'task_id, unit_id, assignment_id, session_id, dt_start,dt_end, keyboard, mouse, scroll, scroll_top, text_selected\n', function(err) {});

var LOGS = [];

var MaxUnit = 0;
// -------------------------------------------------------
// Convert log object into a string
// -------------------------------------------------------
function stringify(log_array) {
    for (var i = 0; i < log_array.length; i++) {
        log_array[i]['string'] = "";
        log_array[i]['string'] += log_array[i].task_id + ", ";
        log_array[i]['string'] += log_array[i].unit_id + ", ";
        log_array[i]['string'] += log_array[i].assignment_id + ", ";
        log_array[i]['string'] += log_array[i].session_id + ", ";
        log_array[i]['string'] += log_array[i].dt_start + ", ";
        log_array[i]['string'] += log_array[i].dt_end + ", ";
        log_array[i]['string'] += log_array[i].keyboard + ", ";
        log_array[i]['string'] += log_array[i].mouse + ", ";
        log_array[i]['string'] += log_array[i].scroll + ", ";
        log_array[i]['string'] += log_array[i].scroll_top + ", ";
        log_array[i]['string'] += log_array[i].text_selected;
    }
    return log_array;
}

function endify(log_array) {
    var Logs = log_array;
    for (var i = 0; i < Logs.length; i++) {
        if (Logs[i].status != 'closed') {
            if (i < (Logs.length - 1) && Logs[i].assignment_id == Logs[i + 1].assignment_id && Logs[i].session_id == Logs[i + 1].session_id) {
                Logs[i]['dt_end'] = Logs[i + 1]['dt_start'];
            }
        }
        if (Logs[i]['dt_end'] == undefined) {
            Logs[i]['dt_end'] = Logs[i]['dt_start'] + 1000;
        }

    }
    return Logs;
}
// -------------------------------------------------------
// Collect data for a given Job and make a plain Array of log objects
// -------------------------------------------------------
requestify.get(firebase_target_url, {
        headers: {
            "Accept": "application/json"
        }
    })
    .then(function(response) {
        var job_logs = response.getBody();
        var units = job_logs.units
        for (var unit_id in units) {
            var assignments = units[unit_id]['assignments'];
            for (var assignment_id in assignments) {
                var sessions = assignments[assignment_id]['sessions'];
                for (var session_id in sessions) {
                    var logs = sessions[session_id]['page_activity'];
                    //console.log(logs);
                    for (var log_id in logs) {
                        var log_record = {
                            task_id: task_id,
                            unit_id: unit_id,
                            assignment_id: assignment_id,
                            session_id: session_id,
                            dt_start: logs[log_id].dt,
                            keyboard: logs[log_id].keyboard,
                            mouse: logs[log_id].mouse,
                            scroll: logs[log_id].scroll,
                            scroll_top: Math.round(logs[log_id].scroll_top),
                            text_selected: logs[log_id].text_selected
                        };
                        LOGS.push(log_record);
                    }
                }
            }
        }
        LOGS = endify(LOGS);
        LOGS = stringify(LOGS);
        console.log(LOGS.length);
        var all_text = "";
        for (var i = 0; i < LOGS.length; i++) {
            all_text+=LOGS[i]['string'] + '\n';
        }
        fs.appendFile(filename, all_text, function(err) {
            if (err)
                console.log(err);
        });
    });
