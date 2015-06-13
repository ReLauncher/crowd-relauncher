var log = require('npmlog');
var moduletitle = 'crowdflower'
module.exports = {
    base: 'https://api.crowdflower.com/',
    version: 'v1/',
    getEndpoint: function(api_key, resource, format, ext) {
        if (!format) {
            format = '.json';
        }
        if (!ext) {
            ext = '';
        }
        var url = this.base + this.version + resource + format + '?key=' + api_key + ext;
        log.info(moduletitle,url)
        return url;
    }
};
