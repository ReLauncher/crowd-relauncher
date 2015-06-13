(function() {
    var app = angular.module('flower-directives', []);

    app.directive("job", function() {
        return {
            restrict: 'E',
            templateUrl: "views/directives/job.html"
        };
    });
})();
