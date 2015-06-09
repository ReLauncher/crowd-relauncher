var express = require('express');
var app = express();

// Connection URL

var opbeat = require('opbeat')({
    organizationId: process.env.OPBEAT_ORGANIZATION_ID,
    appId: process.env.OPBEAT_APP_ID,
    secretToken: process.env.OPBEAT_SECRET_TOKEN
});

var logger = require('./logger');
app.use(logger);

//CORS middleware
var allowCrossDomain = function(req, res, next) {
    res.header('Access-Control-Allow-Origin', 'http://localhost');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type');

    next();
}
app.use(allowCrossDomain);
app.use(express.static('Public'));

var jobs = require('./routes/jobs');
app.use('/jobs', jobs);

var port = process.env.PORT || 3000;
app.listen(port, function() {
    console.log('Listening on '+port);
});
