var express = require('express');
var requestify = require('requestify');
var router = express.Router();
var _ = require('lodash');

var bodyParser = require('body-parser');
var parseBlockName = require('./parse-block-name')();


var resetRoute = router.route('/reset');
resetRoute.get(function(request, response) {
    response.redirect('/');
});

var parseUrlencoded = bodyParser.json();

router.route('/')
    .post(parseUrlencoded, function(request, response) {
        var crowdflower_user = request.body;
        requestify.get('https://api.crowdflower.com/v1/jobs.json?key=' + crowdflower_user.api_key)
            .then(function(crowdflower_resp) {
                // Get the response body (JSON parsed or jQuery object for XMLs)
                var jobs = crowdflower_resp.getBody();
                response.json(jobs);
            });
    });
router.route('/:name')
    .all(parseBlockName)
    .get(function(request, response) {
        var description = blocks[request.blockName];

        if (!description) {
            response.status(404).json('No description found for ' + request.model);
        } else {
            response.json(description);
        }
    })
    .delete(function(request, response) {
        delete blocks[request.blockName];
        response.sendStatus(200);
    });

module.exports = router;
